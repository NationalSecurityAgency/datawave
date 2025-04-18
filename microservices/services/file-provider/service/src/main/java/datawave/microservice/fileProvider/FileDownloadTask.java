package datawave.microservice.fileProvider;

import datawave.microservice.fileProvider.config.FileConfigProperties;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.*;

public class FileDownloadTask implements Runnable {
    private static final Logger log = Logger.getLogger(FileDownloadTask.class);

    private final FileConfigProperties.FileConfig cfg;
    private final String tempDir;

    public FileDownloadTask(FileConfigProperties.FileConfig cfg, String tempDir) {
        this.cfg     = cfg;
        this.tempDir = tempDir;
    }

    @Override
    public void run() {
        String method = cfg.getDownload().getMethod().toLowerCase();
        String source = cfg.getDownload().getSource();
        Path target   = Paths.get(tempDir, cfg.getName());

        try {
            Files.createDirectories(target.getParent());

            switch (method) {
                case "http":
                case "https":
                    downloadHttp(source, target);
                    break;
                case "ftp":
                    downloadFtp(source, target);
                    break;
                default:
                    log.warn("Unsupported method “" + method + "” for " + cfg.getLabel());
            }
        } catch (Exception e) {
            log.error("Failed to download “" + cfg.getLabel() + "” from " + source, e);
        }
    }

    private void downloadHttp(String urlStr, Path target) throws IOException {
        URL url = new URL(urlStr);
        try (InputStream in = url.openStream()) {
            Files.copy(in, target, StandardCopyOption.REPLACE_EXISTING);
            log.info("Downloaded “" + cfg.getLabel() + "” to " + target);
        }
    }

    private void downloadFtp(String ftpUrl, Path target) {
        // If you need FTP, you can use Apache Commons Net:
        // FTPClient ftp = new FTPClient();
        // parse ftpUrl for host/user/pass/path, connect, login, retrieve file, etc.
        log.error("FTP not implemented yet for " + cfg.getLabel());
    }
}
