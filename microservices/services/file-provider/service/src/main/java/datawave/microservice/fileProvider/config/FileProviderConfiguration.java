package datawave.microservice.fileProvider.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

@Configuration
@EnableScheduling
public class FileProviderConfiguration {
    
    // Allows the specified task scheduler instance to be given.
    @Autowired
    public TaskScheduler taskScheduler() {
        return new ThreadPoolTaskScheduler();
    }
}
