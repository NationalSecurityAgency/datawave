package datawave.query.jexl.lookups;

import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

import datawave.core.iterators.FieldExpansionIterator;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.tables.ScannerFactory;
import datawave.util.time.DateHelper;

/**
 * An {@link IndexLookup} that wraps {@link FieldExpansionIterator}.
 */
public class FieldExpansionIndexLookup extends AsyncIndexLookup {

    private static final Logger log = LoggerFactory.getLogger(FieldExpansionIndexLookup.class);

    protected String term;
    protected Future<Boolean> timedScanFuture;
    protected AtomicLong lookupStartTimeMillis = new AtomicLong(Long.MAX_VALUE);
    protected CountDownLatch lookupStartedLatch;
    protected CountDownLatch lookupStoppedLatch;

    private Scanner scanner;

    public FieldExpansionIndexLookup(ShardQueryConfiguration config, ScannerFactory scannerFactory, String term, Set<String> fields,
                    ExecutorService execService) {
        super(config, scannerFactory, true, execService);
        this.term = term;
        this.fields = new HashSet<>();
        if (fields != null) {
            this.fields.addAll(fields);
        }
    }

    @Override
    public void submit() {
        if (indexLookupMap == null) {
            indexLookupMap = new IndexLookupMap(config.getMaxUnfieldedExpansionThreshold(), config.getMaxValueExpansionThreshold());

            try {
                scanner = scannerFactory.newSingleScanner(config.getIndexTableName(), config.getAuthorizations(), config.getQuery());

                Range range = getScanRange();
                scanner.setRange(range);

                for (String field : fields) {
                    scanner.fetchColumnFamily(new Text(field));
                }

                IteratorSetting setting = createIteratorSetting();
                scanner.addScanIterator(setting);

                timedScanFuture = execService.submit(createTimedCallable(scanner));
            } catch (Exception e) {
                log.error("Error expanding term into discrete fields", e);
                // close scanner in case of an exception prior to execution of future
                scannerFactory.close(scanner);
                throw new RuntimeException(e);
            }
            // Note: scanners should never be closed here in a 'finally' block. The createTimedCallable
            // method will close the scanner via scannerFactory.close(scanner)
        }
    }

    private Range getScanRange() {
        Preconditions.checkNotNull(term);
        Key startKey = new Key(term);
        return new Range(startKey, true, startKey.followingKey(PartialKey.ROW), false);
    }

    private IteratorSetting createIteratorSetting() {
        int priority = config.getBaseIteratorPriority() + 24;
        IteratorSetting setting = new IteratorSetting(priority, FieldExpansionIterator.class.getSimpleName(), FieldExpansionIterator.class);

        setting.addOption(FieldExpansionIterator.START_DATE, DateHelper.format(config.getBeginDate()));
        setting.addOption(FieldExpansionIterator.END_DATE, DateHelper.format(config.getEndDate()));

        if (!config.getDatatypeFilter().isEmpty()) {
            setting.addOption(FieldExpansionIterator.DATATYPES, Joiner.on(',').join(config.getDatatypeFilter()));
        }

        if (!fields.isEmpty()) {
            setting.addOption(FieldExpansionIterator.FIELDS, Joiner.on(',').join(fields));
        }

        return setting;
    }

    @Override
    public IndexLookupMap lookup() {
        try {
            timedScanWait(timedScanFuture, lookupStartedLatch, lookupStoppedLatch, lookupStartTimeMillis, config.getMaxAnyFieldScanTimeMillis());
        } finally {
            if (scanner != null) {
                scannerFactory.close(scanner);
            }
        }

        return indexLookupMap;
    }

    protected Callable<Boolean> createTimedCallable(final Scanner scanner) {
        lookupStartedLatch = new CountDownLatch(1);
        lookupStoppedLatch = new CountDownLatch(1);

        return () -> {
            try {
                lookupStartTimeMillis.set(System.currentTimeMillis());
                lookupStartedLatch.countDown();

                final Text holder = new Text();

                try {
                    for (Entry<Key,Value> entry : scanner) {
                        // check for interrupt which may be triggered by closing the batch scanner
                        if (Thread.interrupted()) {
                            throw new InterruptedException();
                        }

                        if (log.isTraceEnabled()) {
                            log.trace("Index entry: {}", entry.getKey());
                        }

                        entry.getKey().getRow(holder);
                        String row = holder.toString();

                        entry.getKey().getColumnFamily(holder);
                        String columnFamily = holder.toString();

                        // We are only returning a mapping of field name to field value, no need to
                        // determine cardinality and such at this point.
                        if (log.isTraceEnabled()) {
                            log.trace("put {}:{}", columnFamily, row);
                        }
                        indexLookupMap.put(columnFamily, row);

                        // if we passed the term expansion threshold, then simply return
                        if (indexLookupMap.isKeyThresholdExceeded()) {
                            break;
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    scannerFactory.close(scanner);
                }

                return true;
            } finally {
                lookupStoppedLatch.countDown();
            }
        };
    }
}
