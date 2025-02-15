package datawave.next.scanner;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.core.query.configuration.QueryData;
import datawave.core.query.configuration.Result;
import datawave.next.async.RunnableWithContext;
import datawave.next.stats.ScanTimeStats;
import datawave.query.Constants;
import datawave.query.iterator.QueryOptions;

public class DocumentRangeScan implements RunnableWithContext {

    private static final Logger log = LoggerFactory.getLogger(DocumentRangeScan.class);

    private final KeyWithContext keyWithContext;
    private final DocumentScannerConfig config;
    private final Authorizations auths;
    private final ArrayBlockingQueue<Result> queue;
    private final AtomicInteger numDocScans;

    private String context;

    private final ScanTimeStats stats;

    public DocumentRangeScan(KeyWithContext keyWithContext, DocumentScannerConfig config) {
        this.keyWithContext = keyWithContext;
        this.config = config;
        this.queue = config.getResults();
        this.auths = config.getAuthorizations();
        this.numDocScans = config.getNumDocScans();
        this.stats = new ScanTimeStats();
        this.stats.markSubmit();
    }

    @Override
    public void run() {
        try {
            Thread.currentThread().setName(getContext());
            stats.markStart();
            if (log.isDebugEnabled()) {
                log.debug("executing document range {}", keyWithContext.getKey().toStringNoTime());
            }

            Range range = createRange();
            IteratorSetting setting = createScanIterator();
            IteratorSetting appliedSettings = config.getVisitorFunction().apply(setting, Collections.singleton(range));

            int numResults = 0;
            try (Scanner scanner = config.getClient().createScanner(keyWithContext.getContext().getTableName(), auths)) {
                scanner.setRange(range);
                scanner.addScanIterator(appliedSettings);

                // should only generate one entry because this is a document range
                // but you know what they say about assumptions.
                for (Map.Entry<Key,Value> entry : scanner) {
                    numResults++;
                    Result result = new Result(entry.getKey(), entry.getValue());

                    boolean offered = false;
                    while (!offered) {
                        try {
                            offered = queue.offer(result, 1, TimeUnit.MILLISECONDS);
                        } catch (InterruptedException e) {
                            throw new RuntimeException("Interrupted while offering result", e);
                        }
                    }
                }

            } catch (Exception e) {
                log.error("exception while fetching document", e);
                throw new RuntimeException(e);
            } finally {
                stats.markStop();
                long elapsed = TimeUnit.NANOSECONDS.toMillis(stats.getScanTime());
                if (log.isDebugEnabled()) {
                    log.debug("num results: {} in {} ms", numResults, elapsed);
                }
                numDocScans.getAndDecrement();
            }
        } catch (Exception e) {
            log.error("error executing document range {}", keyWithContext.getKey().toStringNoTime(), e);
        } finally {
            config.getStats().merge(stats);
        }
    }

    private Range createRange() {
        Key start = keyWithContext.getKey();
        Key stop = new Key(start.getRow(), new Text(start.getColumnFamily().toString() + Constants.MAX_UNICODE_STRING));
        return new Range(start, true, stop, false);
    }

    private IteratorSetting createScanIterator() {
        // copy the query data because it is a shared object
        QueryData queryData = keyWithContext.getContext();
        IteratorSetting orig = queryData.getSettings().get(0);

        // copy original iterator setting
        IteratorSetting setting = new IteratorSetting(orig.getPriority(), orig.getName(), orig.getIteratorClass());
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
