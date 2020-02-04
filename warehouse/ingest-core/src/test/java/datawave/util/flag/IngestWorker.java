package datawave.util.flag;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jline.internal.Log;

/**
 * (U) Worker thread for moving ingest files from the native file system into the ingest directory for the specific ingest data type.
 */
class IngestWorker implements Callable<Void> {
    
    private static final Logger logger = LoggerFactory.getLogger(IngestWorker.class);
    
    @Override
    public Void call() throws InterruptedException {
        final IngestConfig cfg = IngestConfig.getInstance();
        final FileSystem fs = cfg.getHdfs();
        final Path dst = new Path(cfg.getHdfsIngestDir());
        final long endTime = cfg.getDurtion() + System.currentTimeMillis();
        logger.info("starting task thread(" + Thread.currentThread().getId() + ") duration(" + cfg.getDurtion() + ")ms");
        while (endTime >= System.currentTimeMillis()) {
            final List<Path> moveFiles = cfg.getRandomIngestFiles();
            Path[] srcFiles = new Path[moveFiles.size()];
            srcFiles = moveFiles.toArray(srcFiles);
            try {
                fs.copyFromLocalFile(false, false, srcFiles, dst);
            } catch (IOException ioe) {
                Log.error("thread(" + Thread.currentThread().getId() + ") unable to copy ingest files: " + ioe.getMessage());
            }
            final int waitDur = cfg.getRandomInterval();
            Thread.sleep(waitDur);
        }
        
        logger.info("task thread(" + Thread.currentThread().getId() + ") completed");
        return null;
    }
}
