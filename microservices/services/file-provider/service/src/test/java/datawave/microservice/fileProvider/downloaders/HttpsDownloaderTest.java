package datawave.microservice.fileProvider.downloaders;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

class HttpsDownloaderTest {

    /** Remote file location */
    private String downloadURL = "https://data.ny.gov/api/views/d6yy-54nr/rows.csv?accessType=DOWNLOAD";
    /** Local directory to save into (no trailing slash required) */
    private String destinationPath = "/home/ssmucker/Downloads";
    /** File name to use when saving */
    private String destFileName   = "myDownloadedFile.csv";

//    @TempDir
//    Path tempDir;

    @Test
    public void testDownload(@TempDir Path tempDir) throws IOException {

        Path tFile = tempDir.resolve(destFileName);

        HttpsDownloader downloader = new HttpsDownloader();
        downloader.setDownloadURL(downloadURL);
        downloader.setDestFileName(destFileName);
        downloader.setDestinationPath(tempDir.toString());
        downloader.download();

        Assertions.assertTrue(Files.exists(tFile), "File should exist");
    }

}
