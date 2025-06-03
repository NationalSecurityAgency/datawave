package datawave.next.scanner;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Queues;

import datawave.core.query.configuration.QueryData;
import datawave.core.query.configuration.Result;
import datawave.core.query.logic.QueryCheckpoint;
import datawave.core.query.logic.QueryKey;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.scheduler.PushdownScheduler;
import datawave.query.scheduler.Scheduler;
import datawave.query.tables.ScannerFactory;
import datawave.query.tables.async.event.VisitorFunction;
import datawave.query.tables.stats.ScanSessionStats;
import datawave.security.util.AuthorizationsMinimizer;

/**
 * An alternate to the {@link PushdownScheduler} that splits query execution into two stages, finding documents and aggregating documents.
 * <p>
 * Separate executor pools are used to scan the field index and scan the event column.
 */
public class DocumentScheduler extends Scheduler {

    private static final Logger log = LoggerFactory.getLogger(DocumentScheduler.class);

    protected final Iterator<QueryData> queryDataIterator;

    protected final DocumentScannerConfig config;
    protected DocumentScanner scanner;
    protected VisitorFunction visitorFunction;

    public DocumentScheduler(ShardQueryConfiguration config) {
        this.config = config.getDocumentScannerConfig();
        this.config.setClient(config.getClient());
        this.config.setAuthorizations(AuthorizationsMinimizer.minimize(config.getAuthorizations()).iterator().next());
        this.config.setQueryId(config.getQuery().getId().toString());

        this.queryDataIterator = config.getQueriesIter();
    }

    public void setVisitorFunction(VisitorFunction visitorFunction) {
        this.visitorFunction = visitorFunction;
    }

    @Override
    public BatchScanner createBatchScanner(ShardQueryConfiguration config, ScannerFactory scannerFactory, QueryData qd) throws TableNotFoundException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public ScanSessionStats getSchedulerStats() {
        return null;
    }

    @Override
    public List<QueryCheckpoint> checkpoint(QueryKey queryKey) {
        return List.of();
    }

    @Override
    public Iterator<Result> iterator() {
        if (scanner == null) {
            // initialize some objects in the config object
            if (config.isSortedCandidateQueue()) {
                config.setCandidateQueue(Queues.newPriorityBlockingQueue());
            } else {
                config.setCandidateQueue(Queues.newLinkedBlockingDeque());
            }

            config.setResults(Queues.newLinkedBlockingDeque());

            scanner = createScanner();
        }
        return scanner;
    }

    protected DocumentScanner createScanner() {
        DocumentScanner scanner = new DocumentScanner(config, queryDataIterator);
        scanner.setVisitorFunction(visitorFunction);

        // no time like the present
        scanner.start();
        return scanner;
    }

    @Override
    public void close() throws IOException {
        scanner.close();

        logSchedulerStats();
    }

    private void logSchedulerStats() {
        log.info("{}", config.getStats().logStats(config.getQueryId()));
    }
}
