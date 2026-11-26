package com.duoc.ms_vetcare_kafka_admin.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.security.web.SecurityFilterChain;

/** Solo para uso local. Para despliegue público, integrar el IDaaS/JWT de VetCare. */
@Configuration
public class AdminSecurityConfig {
    @Bean
    SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
        http.csrf(AbstractHttpConfigurer::disable)
                .authorizeHttpRequests(auth -> auth
                        .requestMatchers(HttpMethod.OPTIONS, "/**").denyAll()
                        .anyRequest().hasRole("KAFKA_ADMIN"))
                .httpBasic(Customizer.withDefaults())
                .sessionManagement(session -> session
                        .sessionCreationPolicy(SessionCreationPolicy.STATELESS));
        return http.build();
    }

    @Bean
    PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }

    @Bean
    UserDetailsService userDetailsService(
            @Value("${vetcare.admin.username}") String username,
            @Value("${vetcare.admin.password}") String password,
            PasswordEncoder encoder) {
        if (username == null || username.isBlank() ||
                password == null || password.isBlank() || password.length() < 12) {
            throw new IllegalStateException(
                    "Configura VETCARE_ADMIN_USERNAME y VETCARE_ADMIN_PASSWORD (min. 12 caracteres)");
        }
        return new InMemoryUserDetailsManager(
                User.withUsername(username)
                        .password(encoder.encode(password))
                        .roles("KAFKA_ADMIN")
                        .build());
    }
}
