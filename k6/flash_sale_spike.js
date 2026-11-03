import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

// Custom metrics
const successCounter = new Counter('flashgate_successful_reservations');
const soldOutCounter = new Counter('flashgate_sold_out_responses');
const conflictCounter = new Counter('flashgate_conflict_responses');
const failureCounter = new Counter('flashgate_failed_requests');
const reservationLatency = new Trend('flashgate_reserve_duration_ms', true);

export const options = {
    scenarios: {
        flash_sale_spike: {
            executor: 'ramping-vus',
            startVUs: 0,
            stages: [
                { duration: '5s', target: 500 },     // Quick ramp to 500 VUs
                { duration: '15s', target: 2000 },   // Spike to 2,000 VUs
                { duration: '5s', target: 0 },       // Cool down
            ],
            gracefulRampDown: '5s',
        },
    },
    thresholds: {
        'http_req_failed': ['rate<0.05'], // Max 5% unexpected server errors (409/410/202 are expected domain responses)
        'flashgate_reserve_duration_ms': ['p(95)<15', 'p(99)<30'],
    },
};

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const CAMPAIGN_ID = __ENV.CAMPAIGN_ID || '8f9d2a10-4b3c-4e1a-9f0c-123456789abc';
const CONCURRENCY_MODE = __ENV.CONCURRENCY_MODE || 'REDIS_KAFKA_PIPELINE';

function uuidv4() {
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
        const r = (Math.random() * 16) | 0;
        const v = c === 'x' ? r : (r & 0x3) | 0x8;
        return v.toString(16);
    });
}

export default function () {
    const userId = uuidv4();
    const idempotencyKey = `idemp-${__VU}-${__ITER}-${Date.now()}`;

    const headers = {
        'Content-Type': 'application/json',
        'X-User-Id': userId,
        'Idempotency-Key': idempotencyKey,
        'X-Concurrency-Mode': CONCURRENCY_MODE,
    };

    const url = `${BASE_URL}/api/v1/campaigns/${CAMPAIGN_ID}/reserve`;
    const start = Date.now();
    const res = http.post(url, null, { headers });
    const duration = Date.now() - start;

    reservationLatency.add(duration);

    if (res.status === 202 || res.status === 200) {
        successCounter.add(1);
    } else if (res.status === 410) {
        soldOutCounter.add(1);
    } else if (res.status === 409) {
        conflictCounter.add(1);
    } else {
        failureCounter.add(1);
    }

    check(res, {
        'valid flash-sale status code (200, 202, 409, 410)': (r) =>
            r.status === 200 || r.status === 202 || r.status === 409 || r.status === 410,
    });
}
