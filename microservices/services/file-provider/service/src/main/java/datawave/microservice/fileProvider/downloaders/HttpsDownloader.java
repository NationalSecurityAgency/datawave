package datawave.microservice.fileProvider.downloaders;

import datawave.microservice.fileProvider.downloaders.DownloadResult;
import datawave.microservice.fileProvider.downloaders.Downloader;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
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
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

public class HttpsDownloader implements Downloader {

    public static String HTTPS_CODE = "https_code";

    private String downloadURL = "https://data.ny.gov/api/views/d6yy-54nr/rows.csv?accessType=DOWNLOAD";
    private String destinationPath = "~/Downloads";
    private String destFileName = "myDownloadedFile";

    // Need properties for:
    // - url to file
    // - file destination path
    // - file name


    @Override
    public DownloadResult download() throws IOException {

        URL website = new URL(downloadURL);
        try (InputStream in = website.openStream()) {
            Files.copy(in, Path.of(destinationPath + "/" + destFileName), StandardCopyOption.REPLACE_EXISTING);
            return new DownloadResult();
        } catch (IOException e) {
            return new DownloadResult();
        }
    }
}
