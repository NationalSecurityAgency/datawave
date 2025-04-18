package datawave.microservice.fileProvider;

import datawave.microservice.fileProvider.config.FileConfigProperties;
import datawave.microservice.fileProvider.downloaders.DownloadResult;
import datawave.microservice.fileProvider.downloaders.Downloader;
import datawave.microservice.fileProvider.downloaders.HttpsDownloader;

import java.io.IOException;

public class FileDownloadTask implements Runnable{
    
    private final FileConfigProperties.FileConfig config;
    private DownloadResult result;

    public FileDownloadTask(FileConfigProperties.FileConfig config) {
        this.config = config;
    }

    /**
     * Downloads the file specified in the config variable. Passes the nitty-gritty logic off to
     * helper methods depending on the download method (http, etc.)
     */
    @Override
    public void run() {

        if(config.getDownload().getMethod().equalsIgnoreCase("http")) {
            result = downloadFromHttp();
        }
        //...

    }

    /**
     * Download via http
     * @return the result of the download.
     */
    private DownloadResult downloadFromHttp() {
        Downloader downloader = new HttpsDownloader();
        return downloader.download();
    }

    public DownloadResult getResult(){
        return result;
    }
}
