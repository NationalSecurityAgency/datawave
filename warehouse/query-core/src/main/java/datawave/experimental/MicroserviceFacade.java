package datawave.experimental;

import datawave.experimental.util.ScannerChunkUtil;
import datawave.query.attributes.Document;
import datawave.query.function.serializer.KryoDocumentSerializer;
import datawave.query.tables.async.ScannerChunk;
import datawave.query.util.MetadataHelper;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.log4j.Logger;

import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Connect the facade directly to an implementation of the query iterator executor. For testing only.
 * <p>
 * The production version would call out to the live microservice's endpoints.
 *
 */
public class MicroserviceFacade implements Iterator<Entry<Key,Value>> {
    
    private static final Logger log = Logger.getLogger(MicroserviceFacade.class);
    
    private final Iterator<List<ScannerChunk>> chunkIter;
    private final QueryIteratorServiceForTests queryIteratorService;
    
    private final AtomicBoolean isSubmitting = new AtomicBoolean(false);
    private final AtomicBoolean isExecuting = new AtomicBoolean(false);
    private final Executor submitThread = Executors.newSingleThreadExecutor();
    private final Executor resultThread = Executors.newSingleThreadExecutor();
    
    private final LinkedBlockingQueue<Entry<Key,Document>> results;
    
    // serialize the documents coming back off the result queue
    private final KryoDocumentSerializer serializer = new KryoDocumentSerializer();
    
    public MicroserviceFacade(Connector conn, Iterator<List<ScannerChunk>> chunkIter, MetadataHelper metadataHelper) {
        this(conn, chunkIter, metadataHelper, 1);
    }
    
    public MicroserviceFacade(Connector conn, Iterator<List<ScannerChunk>> chunkIter, MetadataHelper metadataHelper, int concurrentChunks) {
        this.chunkIter = chunkIter;
        this.results = new LinkedBlockingQueue<>();
        this.queryIteratorService = new QueryIteratorServiceForTests(results, conn, metadataHelper);
        execute();
    }
    
    public void execute() {
        isSubmitting.getAndSet(true);
        isExecuting.getAndSet(true);
        
        // setup result thread
        resultThread.execute(() -> {
            Thread.currentThread().setName("scan id manager thread");
            while (isSubmitting.get() || queryIteratorService.count() > 0) {
                try {
                    Thread.sleep(5);
                } catch (Exception e) {
                    log.error(e.getMessage());
                    e.printStackTrace();
                }
            }
            isExecuting.getAndSet(false);
        });
        
        // setup submit thread
        submitThread.execute(() -> {
            Thread.currentThread().setName("chunk submit thread");
            while (chunkIter.hasNext()) {
                if (queryIteratorService.canPush()) {
                    List<ScannerChunk> chunks = chunkIter.next();
                    for (ScannerChunk chunk : chunks) {
                        String scanId = ScannerChunkUtil.scanIdFromChunk(chunk);
                        if (!queryIteratorService.push(scanId, chunk)) {
                            log.error("Need to handle when a push is rejected");
                            throw new IllegalStateException("query iterator service rejected a push");
                        }
                    }
                } else {
                    try {
                        Thread.sleep(1);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            isSubmitting.getAndSet(false);
        });
    }
    
    @Override
    public boolean hasNext() {
        while (isExecuting.get()) {
            if (!results.isEmpty()) {
                return true;
            }
            
            try {
                Thread.sleep(1);
            } catch (Exception e) {
                log.error("Error while waiting for results to populate queue: " + e.getMessage());
                e.printStackTrace();
            }
        }
        
        if (results.isEmpty()) {
            log.info("No more results, log stats");
            return false;
        } else {
            return true;
        }
    }
    
    @Override
    public Entry<Key,Value> next() {
        try {
            // enforce a 60 minute timeout here
            Entry<Key,Document> entry = results.poll(60, TimeUnit.MINUTES);
            return serializer.apply(entry);
        } catch (Exception e) {
            log.error("Error while polling: " + e.getMessage());
            e.printStackTrace();
        }
        log.error("polled for result that never came back");
        throw new IllegalStateException("Reported next result that never appeared");
    }
}
