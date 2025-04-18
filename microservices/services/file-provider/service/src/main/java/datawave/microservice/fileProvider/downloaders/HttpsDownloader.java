package datawave.microservice.fileProvider.downloaders;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

public class HttpsDownloader implements Downloader {

    /**
     * Identifier for this downloader type
     */
    public static final String HTTPS_CODE = "https_code";

    /**
     * Remote file location
     */
    private String downloadURL = "https://data.ny.gov/api/views/d6yy-54nr/rows.csv?accessType=DOWNLOAD";
    /**
     * Local directory to save into (no trailing slash required)
     */
    private String destinationPath = "/home/ssmucker/Downloads";
    /**
     * File name to use when saving
     */
    private String destFileName = "myDownloadedFile.csv";

    public String getDownloadURL() {
        return downloadURL;
    }

    public void setDownloadURL(String downloadURL) {
        this.downloadURL = downloadURL;
    }

    public String getDestinationPath() {
        return destinationPath;
    }

    public void setDestinationPath(String destinationPath) {
        this.destinationPath = destinationPath;
    }

    public String getDestFileName() {
        return destFileName;
    }

    public void setDestFileName(String destFileName) {
        this.destFileName = destFileName;
    }

    @Override
    public DownloadResult download(){
        DownloadResult result = new DownloadResult();
        try {
            URL website = new URL(downloadURL);
            InputStream in = website.openStream();
            Path target = Path.of(destinationPath, destFileName);
            Files.copy(in, target, StandardCopyOption.REPLACE_EXISTING);
            result.setStatus(DownloadResult.Status.COMPLETE);
        } catch (IOException e) {
            result.setStatus(DownloadResult.Status.ERROR);
        }
        return result;
    }
}
