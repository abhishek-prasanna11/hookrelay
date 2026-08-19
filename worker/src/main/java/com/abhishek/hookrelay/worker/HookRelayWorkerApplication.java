package com.abhishek.hookrelay.worker;

import com.abhishek.hookrelay.common.domain.Event;
import com.abhishek.hookrelay.common.repo.EventRepository;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

/**
 * The delivery worker.
 *
 * <p>Consumes {@code deliveries} and performs signed HTTP POSTs. It has no web server: it is
 * I/O-bound background work, and it scales on queue depth rather than request rate, which is why
 * BLUEPRINT.md §4 makes it a separate deployment from the API.
 *
 * <p>It does <em>not</em> run Flyway. The API owns the schema; the worker validates its entity
 * mappings against whatever the API migrated and refuses to start if they disagree.
 */
@SpringBootApplication(scanBasePackageClasses = {
        HookRelayWorkerApplication.class,
        com.abhishek.hookrelay.common.CommonModule.class
})
@EntityScan(basePackageClasses = Event.class)
@EnableJpaRepositories(basePackageClasses = EventRepository.class)
public class HookRelayWorkerApplication {

    public static void main(String[] args) {
        SpringApplication.run(HookRelayWorkerApplication.class, args);
    }
}
