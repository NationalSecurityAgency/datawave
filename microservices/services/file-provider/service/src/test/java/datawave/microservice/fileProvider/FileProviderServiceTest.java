package datawave.microservice.fileProvider;

import datawave.microservice.fileProvider.config.FileConfigProperties;
import org.junit.jupiter.api.Test;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.io.File;
import java.util.ArrayList;
import java.util.concurrent.ScheduledFuture;

class FileProviderServiceTest {

    @Test
    public void testScheduledDownload(){

        ThreadPoolTaskScheduler s = new ThreadPoolTaskScheduler();
        s.initialize();

        FileProviderService fps = new FileProviderService(s);
        fps.configProperties = new FileConfigProperties();

        FileConfigProperties.FileConfig fileCfg = new FileConfigProperties.FileConfig();
        fileCfg.setName("MyCoolFile");
        fileCfg.setLabel("MyCoolLabel");

        FileConfigProperties.DownloadConfig fileDownloadCfg = new FileConfigProperties.DownloadConfig();
        fileDownloadCfg.setSchedule("* * * * * ?");
        fileDownloadCfg.setMethod("URL");
        fileDownloadCfg.setSource("www.google.com");
        fileCfg.setDownload(new FileConfigProperties.DownloadConfig());

        ArrayList<FileConfigProperties.FileConfig> files = new ArrayList<>();
        files.add(fileCfg);

        fps.configProperties.setFiles(files);

        ScheduledFuture<?> sf = fps.scheduleDownload(fps.configProperties.getFiles().get(0));
        //while(!sf.isDone()){}

        System.out.println("All done async!");

    }

}