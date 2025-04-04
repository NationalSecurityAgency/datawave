package datawave.microservice.fileProvider;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.web.servlet.error.ErrorMvcAutoConfiguration;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;

/**
 * Launcher for the file provider service
 */
@EnableDiscoveryClient
@SpringBootApplication(scanBasePackages = "datawave.microservice", exclude = {ErrorMvcAutoConfiguration.class})
public class FileProviderService {

    @Inject
    private FileConfigProperties configProperties;
    private TaskExecutor taskExecutor;

    public FileProviderService(TaskExecutor taskExecutor) {
        this.taskExecutor = taskExecutor;
    }

    protected class FileDownloadedTask impliments Runnable {

        private FileConfig file;

        protected FileDownloadedTask(FileConfig file){ }

        @Override
        public void run(){

            try (CloseableHttpClient httpClient = HttpClients.createDefault()) {

                HttpGet httpGet = new HttpGet(file.getDownload().getSource());
                httpClient.execute(httpGet, classicHttpResponse -> {

                    int code = classicHttpResponse.getCode();
                    if (code == 200) {
                        HttpEntity entity = classicHttpResponse.getEntity();
                        if (entity != null) {

                            try (
                                InputStream inputStream = entity.getContent();
                                FileOutputStream fileOutputStream = new FileOutputStream(file.getName())
                            )

                            {
                                byte[] dataBuffer = new byte[1024];
                                int bytesRead;
                                while((bytesRead = inputStream.read(dataBuffer)) != -1)
                                {
                                    fileOutputStream.write(dataBuffer, 0, bytesRead);
                                }
                            }
                        }
                        EntityUtils.consume(entity);
                    }
                    return classicHttpResponse;
                });
            }
        }
    }

    public void downloadFromFileConfig(FileConfig fileConfig){
        taskExecutor.execute(new FileDownloadedTask(fileConfig));
    }

}


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