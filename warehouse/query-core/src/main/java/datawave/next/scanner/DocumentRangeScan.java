package datawave.next.scanner;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iteratorsImpl.system.IterationInterruptedException;
import org.apache.accumulo.core.security.Authorizations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

import datawave.core.query.configuration.QueryData;
import datawave.core.query.configuration.Result;
import datawave.next.async.RunnableWithContext;
import datawave.next.retrieval.DocumentIterator;
import datawave.next.retrieval.DocumentIteratorOptions;
import datawave.next.stats.ScanTimeStats;
import datawave.query.iterator.QueryOptions;

/**
 * Retrieves the document specified by the {@link KeyWithContext}.
 * <p>
 * Multiple documents are fetched with the same scanner if a {@link BulkKeyWithContext} is provided instead.
 */
public class DocumentRangeScan implements RunnableWithContext {

    private static final Logger log = LoggerFactory.getLogger(DocumentRangeScan.class);

    private final KeyWithContext keyWithContext;
    private final DocumentScannerConfig config;
    private final Authorizations auths;

    private final long resultQueueOfferTimeMillis;
    private final BlockingQueue<Result> resultQueue;
    private final AtomicInteger numRetrievalScans;
    private final boolean useQueryIterator;

    private String context;

    private final ScanTimeStats stats;

    public DocumentRangeScan(KeyWithContext keyWithContext, DocumentScannerConfig config) {
        this.keyWithContext = keyWithContext;
        this.config = config;
        this.resultQueueOfferTimeMillis = config.getResultQueueOfferTimeMillis();
        this.resultQueue = config.getResults();
        this.auths = config.getAuthorizations();
        this.numRetrievalScans = config.getNumRetrievalScans();
        this.useQueryIterator = config.isUseQueryIterator();

        String context = getRecordId(keyWithContext.getKey());
        this.stats = new ScanTimeStats();
        this.stats.setContext(context);
        this.stats.markSubmit();
    }

    private String getRecordId(Key key) {
        return key.getRow().toString() + " " + key.getColumnFamily().toString();
    }

    @Override
    public void run() {
        Thread.currentThread().setName(getContext());
        try {
            boolean executing = true;
            while (executing) {
                try {
                    stats.markStart();
                    if (log.isDebugEnabled()) {
                        if (keyWithContext instanceof BulkKeyWithContext) {
                            log.debug("executing document batch {}", ((BulkKeyWithContext) keyWithContext).getKeys().size());
                        } else {
                            log.debug("executing document range {}", keyWithContext.getKey().toStringNoTime());
                        }
                    }

                    if (useQueryIterator) {
                        executeQueryIteratorScan();
                    } else {
                        executeDocumentScan();
                    }
                    executing = false;
                } catch (IterationInterruptedException e) {
                    log.warn("time sliced, resubmitting scan for {}", getContext());
                }
            }
        } catch (Exception e) {
            log.error("error executing document range {}", keyWithContext.getKey().toStringNoTime(), e);
            throw new RuntimeException("error retrieving document: " + keyWithContext.getKey().toStringNoTime(), e);
        } finally {
            stats.markStop();
            numRetrievalScans.getAndDecrement();
            config.getStats().merge(stats);
        }
    }

    private Collection<Range> createRange() {
        if (keyWithContext instanceof BulkKeyWithContext) {
            Set<Range> ranges = new HashSet<>();
            for (Key key : ((BulkKeyWithContext) keyWithContext).getKeys()) {
                ranges.add(createRange(key));
            }
            return ranges;
        }
        return Collections.singleton(createRange(keyWithContext.getKey()));
    }

    protected Range createRange(Key key) {
        Key stop = key.followingKey(PartialKey.ROW_COLFAM);
        return new Range(key, true, stop, false);
    }

    private void executeQueryIteratorScan() {
        Collection<Range> ranges = createRange();
        IteratorSetting setting = createScanIterator();
        IteratorSetting appliedSettings = config.getVisitorFunction().apply(setting, ranges);

        try (Scanner scanner = createScanner()) {
            scanner.addScanIterator(appliedSettings);

            for (Range range : ranges) {
                scanner.setRange(range);
                driveScanner(scanner);
            }

        } catch (Exception e) {
            log.error("exception while fetching document", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Create a scanner and configure it with an execution hint and consistency level
     *
     * @return a configured scanner
     * @throws Exception
     *             if something goes wrong
     */
    private Scanner createScanner() throws Exception {
        String tableName = keyWithContext.getContext().getTableName();
        Scanner scanner = config.getClient().createScanner(tableName, auths);

        if (config.getRetrievalScanHintTable() != null && config.getRetrievalScanHintPool() != null) {
            Preconditions.checkArgument(tableName.equals(config.getRetrievalScanHintTable()), "Table name did not match execution hint");
            scanner.setExecutionHints(Map.of("scan_type", config.getRetrievalScanHintPool()));
        }

        if (config.getSearchConsistencyLevel() != null) {
            scanner.setConsistencyLevel(ConsistencyLevel.valueOf(config.getRetrievalConsistencyLevel()));
        }
        return scanner;
    }

    private void executeDocumentScan() {

        Set<String> candidates = new HashSet<>();
        if (keyWithContext instanceof BulkKeyWithContext) {
            for (Key key : ((BulkKeyWithContext) keyWithContext).getKeys()) {
                candidates.add(key.getColumnFamily().toString());
            }
        } else {
            candidates.add(keyWithContext.getKey().getColumnFamily().toString());
        }

        QueryData queryData = keyWithContext.getContext();
        IteratorSetting orig = queryData.getSettings().get(0);
        IteratorSetting setting = new IteratorSetting(orig.getPriority(), DocumentIterator.class.getSimpleName(), DocumentIterator.class);
        setting.addOption(DocumentIterator.CANDIDATES, Joiner.on(',').join(candidates));

        // only copy over the options we need
        copyRequiredOptions(setting, orig);

        // set the query from the query data
        if (setting.getOptions().containsKey(QueryOptions.QUERY)) {
            setting.addOption(QueryOptions.QUERY, queryData.getQuery());
        }

        try (Scanner scanner = config.getClient().createScanner(keyWithContext.getContext().getTableName(), auths)) {
            scanner.addScanIterator(setting);

            Key start = new Key(keyWithContext.getKey().getRow());
            scanner.setRange(new Range(start, true, start.followingKey(PartialKey.ROW), false));

            driveScanner(scanner);

        } catch (Exception e) {
            log.error("exception while fetching document", e);
            throw new RuntimeException(e);
        }
    }

    private void copyRequiredOptions(IteratorSetting target, IteratorSetting source) {
        Set<String> requiredOptions = DocumentIteratorOptions.getRequiredOptionNames();
        Map<String,String> sourceOptions = source.getOptions();
        for (String requiredOption : requiredOptions) {
            String option = sourceOptions.get(requiredOption);
            if (option != null) {
                target.addOption(requiredOption, option);
            }
        }
    }

    protected void driveScanner(Scanner scanner) {
        for (Map.Entry<Key,Value> entry : scanner) {
            Result result = new Result(entry.getKey(), entry.getValue());

            boolean offered = false;
            while (!offered) {
                try {
                    offered = resultQueue.offer(result, resultQueueOfferTimeMillis, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    throw new RuntimeException("Interrupted while offering result", e);
                }
            }
        }
    }

    private IteratorSetting createScanIterator() {
        // copy the query data because it is a shared object
        QueryData queryData = keyWithContext.getContext();
        IteratorSetting orig = queryData.getSettings().get(0);

        // copy original iterator setting
        boolean useDocumentIterator = false;
        IteratorSetting setting;
        if (useDocumentIterator) {
            setting = new IteratorSetting(orig.getPriority(), DocumentIterator.class.getSimpleName(), DocumentIterator.class);
        } else {
            setting = new IteratorSetting(orig.getPriority(), orig.getName(), orig.getIteratorClass());
        }
        setting.addOptions(orig.getOptions());

        // set the query from the query data
        if (setting.getOptions().containsKey(QueryOptions.QUERY)) {
            setting.addOption(QueryOptions.QUERY, queryData.getQuery());
        }

        // serialize query
        setting.addOption(QueryOptions.SERIAL_EVALUATION_PIPELINE, "true");
        setting.addOption(QueryOptions.MAX_EVALUATION_PIPELINES, "1");
        setting.addOption(QueryOptions.MAX_PIPELINE_CACHED_RESULTS, "1");

        return setting;
    }

    @Override
    public void setContext(String context) {
        this.context = context;
    }

    @Override
    public String getContext() {
        return context;
    }

    public ScanTimeStats getStats() {
        return stats;
    }
}
