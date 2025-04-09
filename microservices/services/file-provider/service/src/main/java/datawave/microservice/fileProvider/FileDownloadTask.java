package datawave.microservice.fileProvider;

import datawave.microservice.fileProvider.config.FileConfigProperties;
import datawave.microservice.fileProvider.downloaders.DownloadResult;
import datawave.microservice.fileProvider.downloaders.Downloader;
import datawave.microservice.fileProvider.downloaders.HttpsDownloader;

import java.io.IOException;

public class FileDownloadTask implements Runnable{
    
    private final FileConfigProperties.FileConfig config;
    
    public FileDownloadTask(FileConfigProperties.FileConfig config) {
        this.config = config;
    }
    
    @Override
    public void run() {
        // Validation for file config? For example, sanitizing file names and URLs.

        // set up downloader via config properties. For example, if we were to set up an https downloader, what would we be checking/supplying.
        Downloader downloader = new HttpsDownloader();
       
        // Download the file.
        try {
            DownloadResult result = downloader.download();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // If success: Validation of file with configured validation
        // Cleanup
    }
    
}
