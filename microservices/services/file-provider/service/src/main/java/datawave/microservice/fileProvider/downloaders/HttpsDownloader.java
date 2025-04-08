package datawave.microservice.fileProvider.downloaders;

public class HttpsDownloader implements Downloader {
    
    // Need properties for:
    // - url to file
    // - file destination path
    // - file name
    
    @Override
    public DownloadResult download() {
        // download via https url
        return null;
    }
}
