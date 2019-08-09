package datawave.util.flag;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a load test for the flag maker. The configuration {@link IngestConfig} allows the flag maker to function as several different modes (system, load, or
 * stress). The configuration consists of the following parameters:
 * <ul>
 * <li>duration of the test</li>
 * <li>source directory containing ingest files</li>
 * <li>hdfs flag maker ingest directory</li>
 * <li>hdfs url</li>
 * <li>number of worker threads to distribute the ingest load</li>
 * <li>minimum and maximum number of files for each worker to create for ingest</li>
 * <li>minimum and maximum interval between ingest executions</li>
 * </ul>
 * Each worker thread performs the following (see {@link IngestWorker}):
 *
 * <pre>
 *    while duration of test has not expired
 *       obtain a list of ingest files
 *       copy files into the hdfs ingest directory
 *       wait a random interval before next ingest execution
 * </pre>
 */
public class FlagMakerLoad {
    
    private static final Logger logger = LoggerFactory.getLogger(FlagMakerLoad.class);
    
    private static void usage() {
        logger.info("USAGE: " + FlagMakerLoad.class.getName() + " configFileName");
    }
    
    public static void main(String[] args) throws IOException {
        // load json config file
        if (1 > args.length) {
            usage();
        } else {
            final FlagMakerLoad tester = new FlagMakerLoad(args[0]);
            tester.runTest();
        }
    }
    
    private FlagMakerLoad(final String cfgFile) throws IOException {
        IngestConfig.create(cfgFile);
    }
    
    /**
     * Creates worker threads to perform ingest and waits for worker threads to complete.
     */
    private void runTest() {
        final IngestConfig cfg = IngestConfig.getInstance();
        logger.info("test execution parameters: " + cfg.toJson());
        ExecutorService executor = Executors.newFixedThreadPool(cfg.getWorkers());
        
        try {
            final List<Future<Void>> workers = new ArrayList<>();
            for (int w = 0; w < cfg.getWorkers(); w++) {
                final IngestWorker worker = new IngestWorker();
                final Future<Void> task = executor.submit(worker);
                workers.add(task);
            }
            
            logger.info("waiting for workers to complete");
            for (final Future<Void> worker : workers) {
                try {
                    Void fut = worker.get();
                } catch (ExecutionException | InterruptedException e) {
                    logger.error("task failure", e);
                }
            }
        } finally {
            executor.shutdown();
        }
        logger.info("test complete");
    }
}
