package datawave.microservice.fileProvider.downloaders;

import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;

import java.io.File;
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

    // ───────────────────────────────────
    // Getters & setters
    // ───────────────────────────────────

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

    // ───────────────────────────────────
    // Business logic
    // ───────────────────────────────────

    @Override
    public DownloadResult download() throws IOException {
        DownloadResult result = new DownloadResult();
        URL website = new URL(downloadURL);

        try (InputStream in = website.openStream()) {
            Path target = Path.of(destinationPath, destFileName);
            Files.copy(in, target, StandardCopyOption.REPLACE_EXISTING);
            result.setStatus(DownloadResult.Status.COMPLETE);
        } catch (IOException e) {
            result.setStatus(DownloadResult.Status.ERROR);
        }
        return result;
    }
}
