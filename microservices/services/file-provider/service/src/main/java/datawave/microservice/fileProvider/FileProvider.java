package datawave.microservice.fileProvider;

import datawave.microservice.fileProvider.config.FileConfigProperties;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.stereotype.Service;

@Service
public class FileProvider {
    
    private static final Logger log = Logger.getLogger(FileProvider.class);
    
    private TaskScheduler taskScheduler;
    
    private FileConfigProperties configProperties;
    
    @Autowired
    public FileProvider(FileConfigProperties configProperties, TaskScheduler taskScheduler){
        this.configProperties = configProperties;
        this.taskScheduler = taskScheduler;
    }
    
    public void scheduleTasks() {
        for (FileConfigProperties.FileConfig fileConfig : configProperties.getFiles()) {
            if(isValid(fileConfig)) {
                FileConfigProperties.DownloadConfig downloadConfig = fileConfig.getDownload();
                FileDownloadTask task = new FileDownloadTask(fileConfig);
                log.info("File download for " + fileConfig.getLabel() + " scheduled for " + downloadConfig.getSchedule());
                taskScheduler.schedule(task, new CronTrigger(downloadConfig.getSchedule()));
            }
        }
    }

    private boolean isValid(FileConfigProperties.FileConfig config) {
        return true;
    }
    
}
