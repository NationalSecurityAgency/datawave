package datawave.query.jexl.lookups;

import java.text.MessageFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import datawave.core.common.logging.ThreadConfigurableLogger;
import datawave.core.iterators.TimeoutExceptionIterator;
import datawave.core.iterators.TimeoutIterator;
import datawave.core.query.configuration.Result;
import datawave.query.Constants;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.exceptions.DoNotPerformOptimizedQueryException;
import datawave.query.parser.JavaRegexAnalyzer.JavaRegexParseException;
import datawave.query.tables.BatchResource;
import datawave.query.tables.ScannerFactory;
import datawave.query.tables.ScannerSession;
import datawave.query.tables.SessionOptions;
import datawave.query.util.MetadataHelper;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.PreConditionFailedQueryException;

/**
 * An asynchronous index lookup which looks up concrete values for the specified regex term.
 */
public class RegexIndexLookup extends AsyncIndexLookup {
    private static final Logger log = ThreadConfigurableLogger.getLogger(RegexIndexLookup.class);

    protected MetadataHelper helper;
    protected Set<String> reverseFields;
    protected final Set<String> patterns;

    protected RegexLookupData forwardLookupData = new RegexLookupData();
    protected RegexLookupData reverseLookupData = new RegexLookupData();

    /**
     *
     * @param config
     *            the shard query configuration, not null
     * @param scannerFactory
     *            the scanner factory, not null
     * @param fields
     *            the fields to lookup, not null
     * @param reverseFields
     *            the reverse fields to lookup, not null
     * @param patterns
     *            the regex patterns to lookup, not null
     * @param helper
     *            the metadata helper, not null
     * @param unfieldedLookup
     *            whether this is an unfielded lookup
     * @param execService
     *            the executor service, not null
     */
    public RegexIndexLookup(ShardQueryConfiguration config, ScannerFactory scannerFactory, Set<String> fields, Set<String> reverseFields, Set<String> patterns,
                    MetadataHelper helper, boolean unfieldedLookup, ExecutorService execService) {
        super(config, scannerFactory, unfieldedLookup, execService);
        this.fields = fields;
        this.reverseFields = reverseFields;
        this.patterns = patterns;
        this.helper = helper;
    }

    /**
     *
     * @param config
     *            the shard query configuration, not null
     * @param scannerFactory
     *            the scanner factory, not null
     * @param fieldName
     *            the field to lookup, not null
     * @param patterns
     *            the regex patterns to lookup, not null
     * @param helper
     *            the metadata helper, not null
     * @param execService
     *            the executor service, not null
     */
    public RegexIndexLookup(ShardQueryConfiguration config, ScannerFactory scannerFactory, String fieldName, Set<String> patterns, MetadataHelper helper,
                    ExecutorService execService) {
        this(config, scannerFactory, Collections.singleton(fieldName), Collections.singleton(fieldName), patterns, helper, false, execService);
    }

    @Override
    public synchronized void submit() {
        if (indexLookupMap == null) {
            indexLookupMap = new IndexLookupMap(config.getMaxUnfieldedExpansionThreshold(), config.getMaxValueExpansionThreshold());
            indexLookupMap.setPatterns(patterns);

            Multimap<String,Range> forwardMap = ArrayListMultimap.create(), reverseMap = ArrayListMultimap.create();

            // Loop over all the patterns, classifying them as forward or reverse index satisfiable
            Iterator<Entry<Key,Value>> iter = Collections.emptyIterator();

            ScannerSession bs;

            IteratorSetting fairnessIterator = null;
            if (config.getMaxIndexScanTimeMillis() > 0) {
                // The fairness iterator solves the problem whereby we have runaway iterators as a result of an evaluation that never finds anything
                fairnessIterator = new IteratorSetting(1, TimeoutIterator.class);

                long maxTime = (long) (config.getMaxIndexScanTimeMillis() * 1.25);
                fairnessIterator.addOption(TimeoutIterator.MAX_SESSION_TIME, Long.valueOf(maxTime).toString());
            }

            for (String pattern : patterns) {
                if (config.getDisallowedRegexPatterns().contains(pattern)) {
                    PreConditionFailedQueryException qe = new PreConditionFailedQueryException(DatawaveErrorCode.IGNORE_PATTERN_FOR_INDEX_LOOKUP,
                                    MessageFormat.format("Pattern: {0}", pattern));
                    log.debug(qe);
                    throw new DoNotPerformOptimizedQueryException(qe);
                }

                ShardIndexQueryTableStaticMethods.RefactoredRangeDescription rangeDescription;
                try {
                    rangeDescription = ShardIndexQueryTableStaticMethods.getRegexRange(null, pattern, config.getFullTableScanEnabled(), helper, config);
                } catch (IllegalArgumentException | JavaRegexParseException e) {
                    log.debug("Ignoring pattern that was not capable of being looked up in the index: " + pattern, e);
                    continue;
                } catch (TableNotFoundException e) {
                    log.error(e);
                    throw new DatawaveFatalQueryException(e);
                } catch (ExecutionException e) {
                    throw new DatawaveFatalQueryException(e);
                }
                if (log.isTraceEnabled()) {
                    log.trace("Adding pattern " + pattern);
                    log.trace("Adding pattern " + rangeDescription);
                }
                if (rangeDescription.isForReverseIndex) {
                    reverseMap.put(pattern, rangeDescription.range);
                } else {
                    forwardMap.put(pattern, rangeDescription.range);
                }
            }

            if (!fields.isEmpty() && !forwardMap.isEmpty()) {
                for (String key : forwardMap.keySet()) {
                    Collection<Range> ranges = forwardMap.get(key);
                    try {
                        bs = ShardIndexQueryTableStaticMethods.configureLimitedDiscovery(config, scannerFactory, config.getIndexTableName(), ranges,
                                        Collections.emptySet(), Collections.singleton(key), false, true);

                        bs.setResourceClass(BatchResource.class);
                    } catch (Exception e) {
                        throw new DatawaveFatalQueryException(e);
                    }
                    SessionOptions opts = bs.getOptions();
                    if (null != fairnessIterator) {
                        opts.addScanIterator(fairnessIterator);

                        IteratorSetting cfg = new IteratorSetting(config.getBaseIteratorPriority() + 100, TimeoutExceptionIterator.class);
                        opts.addScanIterator(cfg);

                    }

                    for (String field : fields) {
                        opts.fetchColumnFamily(new Text(field));
                    }

                    forwardLookupData.getSessions().add(bs);
                    iter = Iterators.concat(iter, Result.keyValueIterator(bs));
                }

                forwardLookupData.setTimedScanFuture(execService.submit(createTimedCallable(iter, fields, forwardLookupData, indexLookupMap)));
            }

            if (!reverseFields.isEmpty() && !reverseMap.isEmpty()) {
                for (String key : reverseMap.keySet()) {
                    Collection<Range> ranges = reverseMap.get(key);
                    if (log.isTraceEnabled()) {
                        log.trace("adding " + ranges + " for reverse");
                    }
                    try {
                        bs = ShardIndexQueryTableStaticMethods.configureLimitedDiscovery(config, scannerFactory, config.getReverseIndexTableName(), ranges,
                                        Collections.emptySet(), Collections.singleton(key), true, true);

                        bs.setResourceClass(BatchResource.class);
                    } catch (Exception e) {
                        throw new DatawaveFatalQueryException(e);
                    }
                    SessionOptions opts = bs.getOptions();
                    if (null != fairnessIterator) {
                        opts.addScanIterator(fairnessIterator);
                        opts.addScanIterator(new IteratorSetting(config.getBaseIteratorPriority() + 100, TimeoutExceptionIterator.class));
                    }

                    for (String field : reverseFields) {
                        opts.fetchColumnFamily(new Text(field));
                    }

                    reverseLookupData.getSessions().add(bs);
                    iter = Iterators.concat(iter, Result.keyValueIterator(bs));
                }

                reverseLookupData.setTimedScanFuture(execService.submit(createTimedCallable(iter, reverseFields, reverseLookupData, indexLookupMap)));
            }
        }
    }

    @Override
    public synchronized IndexLookupMap lookup() {
        if (!forwardLookupData.getSessions().isEmpty()) {
            try {
                timedScanWait(forwardLookupData.getTimedScanFuture(), forwardLookupData.getLookupStartedLatch(), forwardLookupData.getLookupStoppedLatch(),
                                forwardLookupData.getLookupStartTimeMillis(), config.getMaxIndexScanTimeMillis());
            } finally {
                for (ScannerSession sesh : forwardLookupData.getSessions()) {
                    scannerFactory.close(sesh);
                }
                forwardLookupData.getSessions().clear();
            }
        }

        if (!reverseLookupData.getSessions().isEmpty()) {
            try {
                timedScanWait(reverseLookupData.getTimedScanFuture(), reverseLookupData.getLookupStartedLatch(), reverseLookupData.getLookupStoppedLatch(),
                                reverseLookupData.getLookupStartTimeMillis(), config.getMaxIndexScanTimeMillis());
            } finally {
                for (ScannerSession sesh : reverseLookupData.getSessions()) {
                    scannerFactory.close(sesh);
                }
                reverseLookupData.getSessions().clear();
            }
        }

        return indexLookupMap;
    }

    protected Callable<Boolean> createTimedCallable(final Iterator<Entry<Key,Value>> iter, final Set<String> fields, RegexLookupData regexLookupData,
                    final IndexLookupMap indexLookupMap) {
        regexLookupData.setLookupStartedLatch(new CountDownLatch(1));
        regexLookupData.setLookupStoppedLatch(new CountDownLatch(1));

        return () -> {
            try {
                regexLookupData.setLookupStartTimeMillis(System.currentTimeMillis());
                regexLookupData.getLookupStartedLatch().countDown();

                Text holder = new Text();
                try {
                    if (log.isTraceEnabled()) {
                        log.trace("Do we have next? " + iter.hasNext());
                    }

                    while (iter.hasNext()) {
                        // check if interrupted which may be triggered by closing a batch scanner
                        if (Thread.interrupted()) {
                            throw new InterruptedException();
                        }

                        Entry<Key,Value> entry = iter.next();

                        if (TimeoutExceptionIterator.exceededTimedValue(entry)) {
                            throw new Exception("Exceeded fair threshold");
                        }

                        Key topKey = entry.getKey();

                        if (log.isTraceEnabled()) {
                            log.trace("Forward Index entry: " + entry.getKey());
                        }

                        // Get the column qualifier from the key. It contains the datatype and normalizer class
                        if (null != topKey.getColumnQualifier()) {
                            String colq = topKey.getColumnQualifier().toString();
                            int idx = colq.indexOf(Constants.NULL);

                            if (idx != -1) {
                                String type = colq.substring(idx + 1);

                                // If types are specified and this type is not in the list, skip it.
                                if (null != config.getDatatypeFilter() && !config.getDatatypeFilter().isEmpty() && !config.getDatatypeFilter().contains(type)) {

                                    if (log.isTraceEnabled()) {
                                        log.trace(config.getDatatypeFilter() + " does not contain " + type);
                                    }
                                    continue;
                                }

                                topKey.getRow(holder);
                                String term;
                                if (regexLookupData == forwardLookupData) {
                                    term = holder.toString();
                                } else {
                                    term = (new StringBuilder(holder.toString())).reverse().toString();
                                }

                                topKey.getColumnFamily(holder);
                                String field = holder.toString();

                                // synchronize access to fieldsToValues
                                synchronized (indexLookupMap) {
                                    // We are only returning a mapping of field value to field name, no need to
                                    // determine cardinality and such at this point.
                                    indexLookupMap.put(field, term);
                                    // conditional states that if we exceed the key threshold OR field name is not null and we've exceeded
                                    // the value threshold for that field name ( in the case where we have a fielded lookup ).
                                    if (indexLookupMap.isKeyThresholdExceeded() || (fields.size() == 1 && indexLookupMap.get(field).isThresholdExceeded())) {
                                        if (log.isTraceEnabled()) {
                                            log.trace("We've passed term expansion threshold");
                                        }
                                        return true;
                                    }
                                }
                            }
                        }
                    }

                } catch (Exception e) {
                    log.info("Failed or Timed out expanding regex: " + e.getMessage());
                    if (log.isDebugEnabled()) {
                        log.debug("Failed or Timed out " + e);
                    }
                    // synchronize access to fieldsToValues
                    synchronized (indexLookupMap) {
                        // Only if not doing an unfielded lookup should we mark all fields as having an exceeded threshold
                        if (!unfieldedLookup) {
                            for (String field : fields) {
                                if (log.isTraceEnabled()) {
                                    log.trace("field is " + field);
                                }
                                indexLookupMap.put(field, "");
                                indexLookupMap.get(field).setThresholdExceeded();
                            }
                        } else {
                            indexLookupMap.setKeyThresholdExceeded();
                        }
                    }
                    return false;
                }
                return true;
            } finally {
                regexLookupData.getLookupStoppedLatch().countDown();
            }
        };
    }

    private static class RegexLookupData {
        private Collection<ScannerSession> sessions = Lists.newArrayList();
        private Future<Boolean> timedScanFuture;
        private CountDownLatch lookupStartedLatch;
        private CountDownLatch lookupStoppedLatch;
        private AtomicLong lookupStartTimeMillis = new AtomicLong(Long.MAX_VALUE);

        public Collection<ScannerSession> getSessions() {
            return sessions;
        }

        public void setSessions(Collection<ScannerSession> sessions) {
            this.sessions = sessions;
        }

        public Future<Boolean> getTimedScanFuture() {
            return timedScanFuture;
        }

        public void setTimedScanFuture(Future<Boolean> timedScanFuture) {
            this.timedScanFuture = timedScanFuture;
        }

        public CountDownLatch getLookupStartedLatch() {
            return lookupStartedLatch;
        }

        public void setLookupStartedLatch(CountDownLatch lookupStartedLatch) {
            this.lookupStartedLatch = lookupStartedLatch;
        }

        public CountDownLatch getLookupStoppedLatch() {
            return lookupStoppedLatch;
        }

        public void setLookupStoppedLatch(CountDownLatch lookupStoppedLatch) {
            this.lookupStoppedLatch = lookupStoppedLatch;
        }

        public AtomicLong getLookupStartTimeMillis() {
            return lookupStartTimeMillis;
        }

        public void setLookupStartTimeMillis(long lookupStartTimeMillis) {
            this.lookupStartTimeMillis.set(lookupStartTimeMillis);
        }
    }
}
