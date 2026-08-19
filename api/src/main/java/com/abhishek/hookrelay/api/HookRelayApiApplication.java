package com.abhishek.hookrelay.api;

import com.abhishek.hookrelay.common.domain.Event;
import com.abhishek.hookrelay.common.repo.EventRepository;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * The ingest API.
 *
 * <p>Entities and repositories live in the {@code common} module, which is outside this class's
 * package, so component scanning would not find them — hence the explicit {@code @EntityScan} and
 * {@code @EnableJpaRepositories}.
 */
@SpringBootApplication(scanBasePackageClasses = {
        HookRelayApiApplication.class,
        com.abhishek.hookrelay.common.CommonModule.class
})
@EntityScan(basePackageClasses = Event.class)
@EnableJpaRepositories(basePackageClasses = EventRepository.class)
@EnableScheduling
public class HookRelayApiApplication {

    public static void main(String[] args) {
        SpringApplication.run(HookRelayApiApplication.class, args);
    }
}
