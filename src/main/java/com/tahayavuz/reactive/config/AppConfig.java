package com.tahayavuz.reactive.config;

import io.r2dbc.spi.ConnectionFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.r2dbc.connection.init.ConnectionFactoryInitializer;
import org.springframework.r2dbc.connection.init.ResourceDatabasePopulator;

import java.time.Clock;

@Configuration
class AppConfig {

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    /**
     * Şemayı açılışta kurar.
     * <p>
     * R2DBC ile Flyway doğrudan çalışmaz (Flyway JDBC ister). Bu proje tek tablodan
     * ibaret olduğu için basit bir başlatıcı yeterli; gerçek bir sistemde şema
     * yönetimi ayrı bir JDBC bağlantısıyla Flyway'e bırakılır.
     */
    @Bean
    ConnectionFactoryInitializer schemaInitializer(ConnectionFactory connectionFactory) {
        ConnectionFactoryInitializer initializer = new ConnectionFactoryInitializer();
        initializer.setConnectionFactory(connectionFactory);
        initializer.setDatabasePopulator(
                new ResourceDatabasePopulator(new ClassPathResource("db/schema.sql")));
        return initializer;
    }
}
