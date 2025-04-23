package datawave.query.jexl.lookups;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import datawave.core.query.configuration.Result;
import datawave.query.Constants;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.tables.ScannerFactory;
import datawave.query.tables.ScannerSession;

/**
 * An asynchronous index lookup which Looks up field names from the index which match the provided set of terms, and optionally limits them to the specified
 * fields
 */
public class FieldNameIndexLookup extends AsyncIndexLookup {
    private static final Logger log = Logger.getLogger(FieldNameIndexLookup.class);

    protected Set<String> terms;

    protected Future<Boolean> timedScanFuture;
    protected AtomicLong lookupStartTimeMillis = new AtomicLong(Long.MAX_VALUE);
    protected CountDownLatch lookupStartedLatch;
    protected CountDownLatch lookupStoppedLatch;

    private final Collection<ScannerSession> sessions = Lists.newArrayList();

    /**
     *
     * @param config
     *            the shard query configuration, not null
     * @param scannerFactory
     *            the scanner factory, not null
     * @param fields
     *            the fields to limit the lookup, may be null
     * @param terms
     *            the terms to match against, not null
     * @param execService
     *            the executor service, not null
     */
    public FieldNameIndexLookup(ShardQueryConfiguration config, ScannerFactory scannerFactory, Set<String> fields, Set<String> terms,
                    ExecutorService execService) {
        super(config, scannerFactory, true, execService);

        this.fields = new HashSet<>();
        if (fields != null) {
            this.fields.addAll(fields);
        }

        this.terms = new HashSet<>(terms);
    }

    @Override
    public void submit() {
        if (indexLookupMap == null) {
            indexLookupMap = new IndexLookupMap(config.getMaxUnfieldedExpansionThreshold(), config.getMaxValueExpansionThreshold());

            Iterator<Entry<Key,Value>> iter = Collections.emptyIterator();

            ScannerSession bs = null;

            try {
                if (!fields.isEmpty()) {
                    for (String term : terms) {

                        Set<Range> ranges = Collections.singleton(ShardIndexQueryTableStaticMethods.getLiteralRange(term));
                        if (config.getLimitAnyFieldLookups()) {
                            log.trace("Creating configureTermMatchOnly");
                            bs = ShardIndexQueryTableStaticMethods.configureTermMatchOnly(config, scannerFactory, config.getIndexTableName(), ranges,
                                            Collections.singleton(term), Collections.emptySet(), false, true);
                        } else {
                            log.trace("Creating configureLimitedDiscovery");
                            bs = ShardIndexQueryTableStaticMethods.configureLimitedDiscovery(config, scannerFactory, config.getIndexTableName(), ranges,
                                            Collections.singleton(term), Collections.emptySet(), false, true);
                        }

                        // Fetch the limited field names for the given rows
                        for (String field : fields) {
                            bs.getOptions().fetchColumnFamily(new Text(field));
                        }

                        sessions.add(bs);

                        iter = Iterators.concat(iter, Result.keyValueIterator(bs));
                    }
                }

                timedScanFuture = execService.submit(createTimedCallable(iter));
            } catch (IOException | InvocationTargetException | NoSuchMethodException | InstantiationException | IllegalAccessException | RuntimeException e) {
                log.error(e);
                // ensure the scanner is cleaned up if no longer listening
                if (bs != null) {
                    bs.close();
                    sessions.remove(bs);
                }
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public synchronized IndexLookupMap lookup() {
        if (!sessions.isEmpty()) {
            try {
                // for field name lookups, we wait indefinitely
                // TODO consider if this really should be Long.MAX_VALUE or some time less. Other index scanners are set to config.getMaxIndexScanTimeMillis().
                // However the code currently can't handle a failure here, where other index lookup failures can conditionally still allow the query to be
                // executed. See UnfieldedIndexExpansionVisitor.expandUnfielded()
                timedScanWait(timedScanFuture, lookupStartedLatch, lookupStoppedLatch, lookupStartTimeMillis, config.getMaxAnyFieldScanTimeMillis());
            } finally {
                for (ScannerSession sesh : sessions) {
                    scannerFactory.close(sesh);
                }
                sessions.clear();
            }
        }

        return indexLookupMap;
    }

    protected Callable<Boolean> createTimedCallable(final Iterator<Entry<Key,Value>> iter) {
        lookupStartedLatch = new CountDownLatch(1);
        lookupStoppedLatch = new CountDownLatch(1);

        return () -> {
            try {
                lookupStartTimeMillis.set(System.currentTimeMillis());
                lookupStartedLatch.countDown();

                final Text holder = new Text();

                try {
                    while (iter.hasNext()) {
                        // check for interrupt which may be triggered by closing the batch scanner
                        if (Thread.interrupted()) {
                            throw new InterruptedException();
                        }

                        Entry<Key,Value> entry = iter.next();
                        if (log.isTraceEnabled()) {
                            log.trace("Index entry: " + entry.getKey());
                        }

                        entry.getKey().getRow(holder);
                        String row = holder.toString();

                        entry.getKey().getColumnFamily(holder);
                        String colfam = holder.toString();

                        entry.getKey().getColumnQualifier(holder);

                        if (config.getDatatypeFilter() != null && !config.getDatatypeFilter().isEmpty()) {
                            try {
                                String dataType = holder.toString().split(Constants.NULL)[1];
                                if (!config.getDatatypeFilter().contains(dataType))
                                    continue;
                            } catch (Exception e) {
                                // skip the bad key
                                continue;
                            }
                        }

                        // We are only returning a mapping of field name to field value, no need to
                        // determine cardinality and such at this point.
                        indexLookupMap.put(colfam, row);

                        // if we passed the term expansion threshold, then simply return
                        if (indexLookupMap.isKeyThresholdExceeded()) {
                            break;
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    for (ScannerSession session : sessions) {
                        scannerFactory.close(session);
                    }
                }

                return true;
            } finally {
                lookupStoppedLatch.countDown();
            }
        };
    }
}
