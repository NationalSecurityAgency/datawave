package datawave.microservice.fileProvider.downloaders;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class HttpsDownloaderTest {

    @Test
    public void testDownload() throws IOException {
        HttpsDownloader downloader = new HttpsDownloader();
        downloader.download();
    }

}