package datawave.microservice.fileProvider;

import datawave.microservice.fileProvider.config.FileConfigProperties;
import org.apache.log4j.Logger;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import javax.inject.Inject;
import java.util.Date;
import java.util.concurrent.ScheduledFuture;

/**
 * Launcher for the file provider service
 */
@EnableDiscoveryClient
public class FileProviderService {

    private static final Logger log = Logger.getLogger(FileProviderService.class);
    private ThreadPoolTaskScheduler taskScheduler;

    @Inject
    public FileConfigProperties configProperties;;

    public FileProviderService(ThreadPoolTaskScheduler scheduler){
        taskScheduler = scheduler;
    }

    /**
     * Runnable task for downloading a single file, given a {@link FileConfigProperties.FileConfig}.
     */
    public class FileDownloadedTask implements Runnable {

        private FileConfigProperties.FileConfig fileConfig;

        public FileDownloadedTask(FileConfigProperties.FileConfig fileConfig) {
            this.fileConfig = fileConfig;
        }

        @Override
        public void run() {
            log.info("FileDownloadTask for ${asdfasdf} started.");

            log.info(fileConfig.getName());

            log.info("FileDownloadTask for ${asdfasdf} complete.");
        }
    }

    public ScheduledFuture<?> scheduleDownload(FileConfigProperties.FileConfig fileConfig){
        return taskScheduler.scheduleAtFixedRate(new FileDownloadedTask(fileConfig), new Date(), 30000);
    }

}
//        private FileConfigProperties.FileConfig file;
//
//        protected FileDownloadedTask(FileConfigProperties.FileConfig file){ }
//
//        @Override
//        public void run(){

//
//    @Inject
//    private FileConfigProperties configProperties;
//    private Scheduler executorService;
//
//    public FileProviderService(ScheduledExecutorService executor) {
//        this.executorService = executor;
//        if (this.executorService == null){
//            this.executorService = Executors.newSingleThreadScheduledExecutor();        }
//    }
//
//    public void downloadFromFileConfig(FileConfigProperties.FileConfig fileConfig){
//        //need to pul the scheudling from the fileconfig. Where do I put it?
//        executorService.recurringTask(new FileDownloadedTask(fileConfig), 1, TimeUnit.valueOf(fileConfig.getDownload().getSchedule()));
//    }
//
//    protected class FileDownloadedTask implements Runnable {
//
//        private FileConfigProperties.FileConfig file;
//
//        protected FileDownloadedTask(FileConfigProperties.FileConfig file){ }
//
//        @Override
//        public void run(){
//
//            try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
//
//                HttpGet httpGet = new HttpGet(file.getDownload().getSource());
//                httpClient.execute(httpGet, classicHttpResponse -> {
//
//                    int code = classicHttpResponse.getCode();
//                    if (code == 200) {
//                        HttpEntity entity = classicHttpResponse.getEntity();
//                        if (entity != null) {
//
//                            try (
//                                    InputStream inputStream = entity.getContent();
//                                    FileOutputStream fileOutputStream = new FileOutputStream(file.getName())
//                            )
//
//                            {
//                                byte[] dataBuffer = new byte[1024];
//                                int bytesRead;
//                                while((bytesRead = inputStream.read(dataBuffer)) != -1)
//                                {
//                                    fileOutputStream.write(dataBuffer, 0, bytesRead);
//                                }
//                            }
//                        }
//                        EntityUtils.consume(entity);
//                    }
//                    return classicHttpResponse;
//                });
//            }
//        }
//    }
//





/*
    Files need to be downloaded from an URL on a recurring basis. -> Spring Scheduler
    The recurring basis of downloading files will need to be configurable. -> Use the FileConfigProperties class to get it
    The location of the url should be configurable. -> USe the FCP class

    When implementing scheduling for downloads be sure to use a scheduling tool rather than implementing our own. -> Spring Scheduler
    Spring has a built in scheduler that may be useful for this. -> Spring Scheduler

    The filename of the downloaded file should be logged out to a log file when the  -> Logger logger start fn
    download is starting and again once complete. -> Logger logger end fn
    Log and Error if the download is unsuccessful. -> Logger logger try catch or smthn

    Below are a couple of files to use for testing

    New York Lottery Winning numbers
    Link: https://catalog.data.gov/dataset/lottery-powerball-winning-numbers-beginning-2010
    Endpoint: https://data.ny.gov/api/views/d6yy-54nr/rows.csv?accessType=DOWNLOAD
    Size: 50K

    Airports under the Department of Transportation
    Link: https://catalog.data.gov/dataset/airports-5e97a
    Endpoint: https://data.bts.gov/views/kfcv-nyy3/rows.csv?accessType=DOWNLOAD
    Size: 12M
 */