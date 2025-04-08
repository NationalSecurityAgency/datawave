package datawave.microservice.fileProvider;

import datawave.microservice.fileProvider.config.FileConfigProperties;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.stereotype.Service;

@Service
public class FileProvider {
    
    private static final Logger log = Logger.getLogger(FileProviderService.class);
    
    private TaskScheduler taskScheduler;
    
    private FileConfigProperties configProperties;
    
    @Autowired
    public FileProvider(FileConfigProperties configProperties, TaskScheduler taskScheduler){
        this.configProperties = configProperties;
        this.taskScheduler = taskScheduler;
    }
    
    public void scheduleTasks() {
        for (FileConfigProperties.FileConfig fileConfig : configProperties.getFiles()) {
            FileConfigProperties.DownloadConfig downloadConfig = fileConfig.getDownload();
            FileDownloadTask task = new FileDownloadTask(fileConfig);
            log.info("File download for " + fileConfig.getLabel() + " scheduled for " + downloadConfig.getSchedule());
            taskScheduler.schedule(task, new CronTrigger(downloadConfig.getSchedule()));
        }
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
