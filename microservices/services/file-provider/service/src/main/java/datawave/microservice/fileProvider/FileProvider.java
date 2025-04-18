package datawave.microservice.fileProvider;

import datawave.microservice.fileProvider.config.FileConfigProperties;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

@Service
public class FileProvider {
    private static final Logger log = Logger.getLogger(FileProvider.class);

    private final TaskScheduler taskScheduler;
    private final FileConfigProperties configProperties;

    @Autowired
    public FileProvider(FileConfigProperties configProperties,
                        TaskScheduler taskScheduler) {
        this.configProperties = configProperties;
        this.taskScheduler   = taskScheduler;
    }

    @PostConstruct
    public void scheduleTasks() {
        for (FileConfigProperties.FileConfig fileConfig : configProperties.getFiles()) {
            if (!isValid(fileConfig)) {
                log.warn("Skipping invalid config: " + fileConfig.getLabel());
                continue;
            }

            String cron = fileConfig.getDownload().getSchedule();
            try {
                taskScheduler.schedule(
                        new FileDownloadTask(fileConfig, configProperties.getTempDir()),
                        new CronTrigger(cron)
                );
                log.info("Scheduled download of “"
                        + fileConfig.getLabel()
                        + "” with cron “" + cron + "”");
            } catch (IllegalArgumentException iae) {
                log.error("Bad cron expression for “"
                        + fileConfig.getLabel()
                        + "”: " + cron, iae);
            }
        }
    }

    /** very basic sanity checks on your config */
    private boolean isValid(FileConfigProperties.FileConfig cfg) {
        return cfg.getLabel() != null && !cfg.getLabel().isEmpty()
                && cfg.getName()  != null && !cfg.getName().isEmpty()
                && cfg.getDownload() != null
                && cfg.getDownload().getMethod()   != null
                && cfg.getDownload().getSource()   != null
                && cfg.getDownload().getSchedule() != null;
    }
}
