package datawave.microservice.fileProvider.downloaders;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

class HttpsDownloaderTest {

    /** Remote file location */
    private String downloadURL = "https://data.ny.gov/api/views/d6yy-54nr/rows.csv?accessType=DOWNLOAD";
    /** File name to use when saving */
    private String destFileName   = "myDownloadedFile.csv";


    @Test
    public void testDownloadedFileExists(@TempDir Path tempDir) throws IOException {

        Path tFile = tempDir.resolve(destFileName);

        HttpsDownloader downloader = new HttpsDownloader();
        downloader.setDownloadURL(downloadURL);
        downloader.setDestFileName(destFileName);
        downloader.setDestinationPath(tempDir.toString());
        downloader.download();

        Assertions.assertTrue(Files.exists(tFile), "File should exist");
    }

    @Test
    public void testDownloadedResultComplete(@TempDir Path tempDir) throws IOException {

        HttpsDownloader downloader = new HttpsDownloader();
        downloader.setDownloadURL(downloadURL);
        downloader.setDestFileName(destFileName);
        downloader.setDestinationPath(tempDir.toString());
        DownloadResult result = downloader.download();

        Assertions.assertSame(DownloadResult.Status.COMPLETE, result.getStatus(), "File should exist");
    }

    @Test
    public void testDownloadedResultBadURL(@TempDir Path tempDir) throws IOException {

        HttpsDownloader downloader = new HttpsDownloader();
        downloader.setDownloadURL("http://www.com");
        downloader.setDestFileName(destFileName);
        downloader.setDestinationPath(tempDir.toString());
        DownloadResult result = downloader.download();

        Assertions.assertSame(DownloadResult.Status.ERROR, result.getStatus(), "File should exist");
    }

}
