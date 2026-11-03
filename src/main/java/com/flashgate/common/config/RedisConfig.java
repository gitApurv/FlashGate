package com.flashgate.common.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.script.DefaultRedisScript;

@Configuration
public class RedisConfig {

    @Bean
    public DefaultRedisScript<String> reserveTicketScript() {
        DefaultRedisScript<String> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource("lua/reserve_ticket.lua"));
        script.setResultType(String.class);
        return script;
    }

    @Bean
    public DefaultRedisScript<Long> releaseTicketScript() {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource("lua/release_ticket.lua"));
        script.setResultType(Long.class);
        return script;
    }
}
