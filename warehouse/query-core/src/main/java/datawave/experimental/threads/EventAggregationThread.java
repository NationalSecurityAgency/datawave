package datawave.experimental.threads;

import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.data.Range;
import org.apache.log4j.Logger;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.FutureCallback;

import datawave.experimental.scanner.event.ConfiguredEventScanner;
import datawave.experimental.scanner.event.EventScanner;
import datawave.query.attributes.Document;

/**
 * Scans events
 */
public class EventAggregationThread implements Runnable, FutureCallback<EventAggregationThread> {

    private static final Logger log = Logger.getLogger(EventAggregationThread.class);

    private final LinkedBlockingQueue<String> uidQueue;
    private final LinkedBlockingQueue<Document> documentQueue;

    private final EventScanner scanner;
    private final Range range;
    private final int batchSize = Integer.MAX_VALUE;

    public EventAggregationThread(LinkedBlockingQueue<String> uidQueue, LinkedBlockingQueue<Document> documentQueue, EventScanner scanner, Range range) {
        this.uidQueue = uidQueue;
        this.documentQueue = documentQueue;
        this.scanner = scanner;
        this.range = range;
    }

    @Override
    public void run() {
        try {
            while (!uidQueue.isEmpty()) {
                try {
                    if (batchSize > 0 && scanner instanceof ConfiguredEventScanner) {
                        SortedSet<String> batchedUids = new TreeSet<>();
                        while (!uidQueue.isEmpty() && batchedUids.size() < batchSize) {
                            String uid = uidQueue.poll(250, TimeUnit.MICROSECONDS);
                            if (uid != null) {
                                batchedUids.add(uid);
                            }
                        }

                        Iterator<Document> documents = ((ConfiguredEventScanner) scanner).fetchDocuments(range, batchedUids);
                        while (documents.hasNext()) {
                            offer(documents.next());
                        }

                    } else {
                        String uid = uidQueue.poll(250, TimeUnit.MICROSECONDS);
                        if (uid != null) {
                            Document d = scanner.fetchDocument(range, uid);
                            offer(d);
                        }
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        } catch (Exception e) {
            log.error(e);
            throw new RuntimeException(e);
        }
        int i = 0;
    }

    private void offer(Document d) {
        boolean accepted = false;
        while (!accepted) {
            try {
                accepted = documentQueue.offer(d, 250, TimeUnit.MICROSECONDS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void onSuccess(EventAggregationThread result) {
        // do nothing
    }

    @Override
    public void onFailure(Throwable t) {
        log.error(t);
        Throwables.propagateIfPossible(t, RuntimeException.class);
    }
}
