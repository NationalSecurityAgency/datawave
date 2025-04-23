package datawave.microservice.config.server;

import org.eclipse.jgit.api.TransportConfigCallback;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.config.server.config.ConfigServerProperties;
import org.springframework.cloud.config.server.environment.EnvironmentRepository;
import org.springframework.cloud.config.server.environment.MultipleJGitEnvironmentProperties;
import org.springframework.cloud.config.server.environment.MultipleJGitEnvironmentRepository;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.ConfigurableEnvironment;

/**
 * Configures the Datawave Config Server by specifying an extended {@link EnvironmentRepository} that handles encrypted private SSH keys.
 */
@Configuration
public class DatawaveConfigServerConfiguration {
    @Autowired
    private ConfigurableEnvironment environment;
    
    @Autowired
    private MultipleJGitEnvironmentProperties properties;
    
    @Autowired
    private ConfigServerProperties server;
    
    @Autowired(required = false)
    private TransportConfigCallback transportConfigCallback;
    
    @Bean
    public EnvironmentRepository environmentRepository() {
        MultipleJGitEnvironmentRepository repository = new DatawaveJGitEnvironmentRepository(environment, properties);
        repository.setTransportConfigCallback(this.transportConfigCallback);
        if (this.server.getDefaultLabel() != null) {
            repository.setDefaultLabel(this.server.getDefaultLabel());
        }
        return repository;
    }
}
