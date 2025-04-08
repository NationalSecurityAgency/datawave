package datawave.microservice.fileProvider;

import datawave.microservice.fileProvider.config.FileConfigProperties;
import datawave.microservice.fileProvider.downloaders.Downloader;
import datawave.microservice.fileProvider.downloaders.HttpsDownloader;

public class FileDownloadTask implements Runnable{
    
    private final FileConfigProperties.FileConfig config;
    
    public FileDownloadTask(FileConfigProperties.FileConfig config) {
        this.config = config;
    }
    
    @Override
    public void run() {
        // set up downloader via config properties. For example, if we were to set up an https downloader, what would we be checking/supplying.
        Downloader downloader = new HttpsDownloader();
       
        // Download the file.
        downloader.download();
        
        //
    }
    
}
