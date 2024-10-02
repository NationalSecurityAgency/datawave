package datawave.query.jexl.lookups;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.springframework.util.StringUtils;

import com.google.common.base.Preconditions;

import datawave.core.common.logging.ThreadConfigurableLogger;
import datawave.core.iterators.ColumnQualifierRangeIterator;
import datawave.core.iterators.CompositeSeekingIterator;
import datawave.core.iterators.TimeoutExceptionIterator;
import datawave.core.iterators.TimeoutIterator;
import datawave.data.type.DiscreteIndexType;
import datawave.query.Constants;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.exceptions.IllegalRangeArgumentException;
import datawave.query.jexl.LiteralRange;
import datawave.query.tables.ScannerFactory;
import datawave.util.time.DateHelper;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.NotFoundQueryException;
import datawave.webservice.query.exception.QueryException;

/**
 * An asynchronous index lookup which looks up concrete values for the specified bounded range.
 */
public class BoundedRangeIndexLookup extends AsyncIndexLookup {
    private static final Logger log = ThreadConfigurableLogger.getLogger(BoundedRangeIndexLookup.class);

    private final LiteralRange<?> literalRange;

    protected Future<Boolean> timedScanFuture;
    protected AtomicLong lookupStartTimeMillis = new AtomicLong(Long.MAX_VALUE);
    protected CountDownLatch lookupStartedLatch;
    protected CountDownLatch lookupStoppedLatch;

    protected BatchScanner bs;

    /**
     *
     * @param config
     *            the shard query configuration, not null
     * @param scannerFactory
     *            the scanner factory, not null
     * @param literalRange
     *            the range to lookup, not null
     * @param execService
     *            the executor service, not null
     */
    public BoundedRangeIndexLookup(ShardQueryConfiguration config, ScannerFactory scannerFactory, LiteralRange<?> literalRange, ExecutorService execService) {
        super(config, scannerFactory, false, execService);
        this.literalRange = literalRange;
        this.fields = Collections.singleton(literalRange.getFieldName());
    }

    @Override
    public synchronized void submit() {
        if (indexLookupMap == null) {
            String startDay = DateHelper.format(config.getBeginDate());
            String endDay = DateHelper.format(config.getEndDate());

            // build the start and end range for the scanner
            // Key for global index is Row-> Normalized FieldValue, CF-> FieldName,
            // CQ->shard_id\x00datatype
            indexLookupMap = new IndexLookupMap(config.getMaxUnfieldedExpansionThreshold(), config.getMaxValueExpansionThreshold());

            IteratorSetting fairnessIterator = null;
            if (config.getMaxIndexScanTimeMillis() > 0) {
                // The fairness iterator solves the problem whereby we have runaway iterators as a result of an evaluation that never finds anything
                fairnessIterator = new IteratorSetting(1, TimeoutIterator.class);

                long maxTime = config.getMaxIndexScanTimeMillis();
                if (maxTime < Long.MAX_VALUE / 2)
                    maxTime *= 2;
                fairnessIterator.addOption(TimeoutIterator.MAX_SESSION_TIME, Long.valueOf(maxTime).toString());

            }

            String lower = literalRange.getLower().toString(), upper = literalRange.getUpper().toString();

            Key startKey;
            if (literalRange.isLowerInclusive()) { // inclusive
                startKey = new Key(new Text(lower));
            } else { // non-inclusive
                startKey = new Key(new Text(lower + "\0"));
            }

            Key endKey;
            if (literalRange.isUpperInclusive()) {
                // we should have our end key be the end of the range if we are going to use the WRI
                endKey = new Key(new Text(upper), new Text(literalRange.getFieldName()), new Text(endDay + Constants.MAX_UNICODE_STRING));
            } else {
                endKey = new Key(new Text(upper));
            }

            Range range;
            try {
                range = new Range(startKey, true, endKey, literalRange.isUpperInclusive());
            } catch (IllegalArgumentException e) {
                QueryException qe = new QueryException(DatawaveErrorCode.RANGE_CREATE_ERROR, e, MessageFormat.format("{0}", this.literalRange));
                log.debug(qe);
                throw new IllegalRangeArgumentException(qe);
            }

            log.debug("Range: " + range);
            bs = null;
            try {
                bs = scannerFactory.newScanner(config.getIndexTableName(), config.getAuthorizations(), config.getNumQueryThreads(), config.getQuery());

                bs.setRanges(Collections.singleton(range));
                bs.fetchColumnFamily(new Text(literalRange.getFieldName()));

                // set up the GlobalIndexRangeSamplingIterator

                IteratorSetting cfg = new IteratorSetting(config.getBaseIteratorPriority() + 50, "WholeRowIterator", WholeRowIterator.class);
                bs.addScanIterator(cfg);

                cfg = new IteratorSetting(config.getBaseIteratorPriority() + 48, "DateFilter", ColumnQualifierRangeIterator.class);
                // search from 20YYddMM to 20ZZddMM\uffff to ensure we encompass all of the current day
                String end = endDay + Constants.MAX_UNICODE_STRING;
                cfg.addOption(ColumnQualifierRangeIterator.RANGE_NAME, ColumnQualifierRangeIterator.encodeRange(new Range(startDay, end)));

                bs.addScanIterator(cfg);

                // If this is a composite field, with multiple terms, we need to setup our query to filter based on each component of the composite range
                if (config.getCompositeToFieldMap().get(literalRange.getFieldName()) != null) {

                    String compositeSeparator = null;
                    if (config.getCompositeFieldSeparators() != null)
                        compositeSeparator = config.getCompositeFieldSeparators().get(literalRange.getFieldName());

                    if (compositeSeparator != null && (lower.contains(compositeSeparator) || upper.contains(compositeSeparator))) {
                        IteratorSetting compositeIterator = new IteratorSetting(config.getBaseIteratorPriority() + 51, CompositeSeekingIterator.class);

                        compositeIterator.addOption(CompositeSeekingIterator.COMPONENT_FIELDS,
                                        StringUtils.collectionToCommaDelimitedString(config.getCompositeToFieldMap().get(literalRange.getFieldName())));

                        for (String fieldName : config.getCompositeToFieldMap().get(literalRange.getFieldName())) {
                            DiscreteIndexType<?> type = config.getFieldToDiscreteIndexTypes().get(fieldName);
                            if (type != null)
                                compositeIterator.addOption(fieldName + CompositeSeekingIterator.DISCRETE_INDEX_TYPE, type.getClass().getName());
                        }

                        compositeIterator.addOption(CompositeSeekingIterator.SEPARATOR, compositeSeparator);

                        bs.addScanIterator(compositeIterator);
                    }
                }

                if (null != fairnessIterator) {
                    cfg = new IteratorSetting(config.getBaseIteratorPriority() + 100, TimeoutExceptionIterator.class);
                    bs.addScanIterator(cfg);
                }

                bs.setExecutionHints(Collections.singletonMap(config.getIndexTableName(), ShardIndexQueryTableStaticMethods.DEFAULT_EXECUTION_HINT));

                timedScanFuture = execService.submit(createTimedCallable(bs.iterator()));
            } catch (TableNotFoundException e) {
                NotFoundQueryException qe = new NotFoundQueryException(DatawaveErrorCode.TABLE_NOT_FOUND, e,
                                MessageFormat.format("Table: {0}", config.getIndexTableName()));
                log.error(qe);
                throw new DatawaveFatalQueryException(qe);

            } catch (IOException e) {
                QueryException qe = new QueryException(DatawaveErrorCode.RANGE_CREATE_ERROR, e, MessageFormat.format("{0}", this.literalRange));
                log.debug(qe);
                if (bs != null) {
                    scannerFactory.close(bs);
                }
                throw new IllegalRangeArgumentException(qe);
            }
        }
    }

    @Override
    public synchronized IndexLookupMap lookup() {
        if (bs != null) {
            try {
                timedScanWait(timedScanFuture, lookupStartedLatch, lookupStoppedLatch, lookupStartTimeMillis, config.getMaxIndexScanTimeMillis());
            } finally {
                scannerFactory.close(bs);
                bs = null;
            }

            if (log.isDebugEnabled()) {
                log.debug("Found " + indexLookupMap.size() + " matching terms for range: " + indexLookupMap);
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

                Text holder = new Text();
                try {
                    if (log.isTraceEnabled()) {
                        log.trace("Do we have next? " + iter.hasNext());
                    }

                    while (iter.hasNext()) {

                        Entry<Key,Value> entry = iter.next();
                        if (TimeoutExceptionIterator.exceededTimedValue(entry)) {
                            throw new Exception("Timeout exceeded for bounded range lookup");
                        }

                        Key k = entry.getKey();

                        if (log.isTraceEnabled()) {
                            log.trace("Forward Index entry: " + entry.getKey());
                        }

                        k.getRow(holder);
                        String uniqueTerm = holder.toString();

                        SortedMap<Key,Value> keymap = WholeRowIterator.decodeRow(entry.getKey(), entry.getValue());

                        String field = null;

                        boolean foundDataType = false;

                        for (Key topKey : keymap.keySet()) {
                            if (null == field) {
                                topKey.getColumnFamily(holder);
                                field = holder.toString();
                            }
                            // Get the column qualifier from the key. It
                            // contains the datatype and normalizer class

                            if (null != topKey.getColumnQualifier()) {
                                if (null != config.getDatatypeFilter() && !config.getDatatypeFilter().isEmpty()) {

                                    String colq = topKey.getColumnQualifier().toString();
                                    int idx = colq.indexOf(Constants.NULL);

                                    if (idx != -1) {
                                        String type = colq.substring(idx + 1);

                                        // If types are specified and this type
                                        // is not in the list, skip it.
                                        if (config.getDatatypeFilter().contains(type)) {
                                            if (log.isTraceEnabled()) {
                                                log.trace(config.getDatatypeFilter() + " contains " + type);
                                            }

                                            foundDataType = true;
                                            break;
                                        }
                                    }
                                } else {
                                    foundDataType = true;
                                }
                            }
                        }
                        if (foundDataType) {

                            // obtaining the size of a map can be expensive,
                            // instead
                            // track the count of each unique item added.
                            indexLookupMap.put(field, uniqueTerm);

                            // safety check...
                            Preconditions.checkState(field.equals(literalRange.getFieldName()),
                                            "Got an unexpected field name when expanding range" + field + " " + literalRange.getFieldName());

                            // If this range expands into to many values, we can
                            // stop
                            if (indexLookupMap.get(field).isThresholdExceeded()) {
                                return true;
                            }
                        }
                    }
                } catch (Exception e) {
                    log.info("Failed or timed out expanding range fields: " + e.getMessage());
                    if (log.isDebugEnabled()) {
                        log.debug("Failed or Timed out ", e);
                    }
                    // Only if not doing an unfielded lookup should we mark all fields as having an exceeded threshold
                    if (!unfieldedLookup) {
                        for (String field : fields) {
                            if (log.isTraceEnabled()) {
                                log.trace("field is " + field);
                                log.trace("field is " + (null == indexLookupMap));
                            }
                            indexLookupMap.put(field, "");
                            indexLookupMap.get(field).setThresholdExceeded();
                        }
                    } else {
                        indexLookupMap.setKeyThresholdExceeded();
                    }
                    return false;
                }
                return true;
            } finally {
                lookupStoppedLatch.countDown();
            }
        };
    }
}
