package datawave.query.jexl.lookups;

import static datawave.query.jexl.lookups.ShardIndexQueryTableStaticMethods.EXPANSION_HINT_KEY;

import java.text.MessageFormat;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import com.google.common.base.Preconditions;

import datawave.core.iterators.BoundedRangeExpansionIterator;
import datawave.core.iterators.CompositeSeekingIterator;
import datawave.data.type.DiscreteIndexType;
import datawave.query.Constants;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.IllegalRangeArgumentException;
import datawave.query.jexl.LiteralRange;
import datawave.query.tables.ScannerFactory;
import datawave.util.time.DateHelper;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;

/**
 * An asynchronous index lookup which looks up concrete values for the specified bounded range.
 * <p>
 * A fielded bounded range is already executable so this lookup is allowed to hit timeout or value thresholds.
 */
public class BoundedRangeIndexLookup extends AsyncIndexLookup {

    private static final Logger log = LoggerFactory.getLogger(BoundedRangeIndexLookup.class);

    private final LiteralRange<?> literalRange;
    protected final String field;

    // variables to support range creation
    private final String startDay;
    private final String endDay;
    private final String lower;
    private final String upper;

    private final Range scanRange;

    protected Future<?> future;

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
        this.field = literalRange.getFieldName();

        // setup variables for creating scan ranges
        this.startDay = DateHelper.format(config.getBeginDate());
        this.endDay = DateHelper.format(config.getEndDate());

        this.lower = literalRange.getLower().toString();
        this.upper = literalRange.getUpper().toString();

        // create the scan range here to surface an illegal range exception
        this.scanRange = createScanRange(lower, upper, endDay);
    }

    /**
     * This method is responsible for creating a Runnable, submitting it to the executor and registering the future with the {@link ScanMonitor}.
     */
    @Override
    public void submit() {
        if (indexLookupMap == null) {
            Preconditions.checkNotNull(monitor, "BoundedRangeIndexLookup requires a ScanMonitor");
            indexLookupMap = new IndexLookupMap(config.getMaxUnfieldedExpansionThreshold(), config.getMaxValueExpansionThreshold());

            Runnable runnable = createRunnable(getTableName(), config.getAuthorizations().iterator().next());
            future = execService.submit(runnable);
            monitor.registerTask(future, config.getMaxIndexScanTimeMillis());
        }
    }

    /**
     * The created runnable handles everything with configuring a scanner, parsing results and putting them into the {@link #indexLookupMap} and handling
     * exceptions.
     * <p>
     * Note: it is critical that any scanner created here is used with a try-with-resources block.
     *
     * @param tableName
     *            the table to be scanned
     * @param auths
     *            the authorizations
     * @return a Runnable that wraps a scanner
     */
    protected Runnable createRunnable(String tableName, Authorizations auths) {
        return () -> {
            try (Scanner scanner = config.getClient().createScanner(tableName, auths)) {
                String hintKey = config.getTableHints().containsKey(EXPANSION_HINT_KEY) ? EXPANSION_HINT_KEY : config.getIndexTableName();
                scanner.setExecutionHints(Map.of(tableName, hintKey));
                scanner.setRange(scanRange);
                scanner.fetchColumnFamily(literalRange.getFieldName());

                IteratorSetting rangeIterator = createRangeIterator(startDay, endDay);
                scanner.addScanIterator(rangeIterator);

                IteratorSetting compositeIterator = createCompositeIterator(lower, upper);
                if (compositeIterator != null) {
                    scanner.addScanIterator(compositeIterator);
                }

                for (Map.Entry<Key,Value> entry : scanner) {
                    Key key = entry.getKey();
                    if (log.isTraceEnabled()) {
                        log.trace("tk: {}", key.toStringNoTime());
                    }
                    indexLookupMap.put(field, key.getRow().toString());
                }

            } catch (Exception e) {
                handleException(e);
            } finally {
                if (log.isTraceEnabled()) {
                    log.trace("closing scanner");
                }
            }
        };
    }

    /**
     * Create the {@link BoundedRangeExpansionIterator} and configure with start and end dates. Optionally configure a datatype filter.
     *
     * @param startDay
     *            the start date
     * @param endDay
     *            the end date
     * @return a {@link BoundedRangeExpansionIterator}
     */
    protected IteratorSetting createRangeIterator(String startDay, String endDay) {
        int priority = config.getBaseIteratorPriority() + 20;
        IteratorSetting setting = new IteratorSetting(priority, "BoundedRangeExpansionIterator", BoundedRangeExpansionIterator.class);
        setting.addOption(BoundedRangeExpansionIterator.START_DATE, startDay);
        setting.addOption(BoundedRangeExpansionIterator.END_DATE, endDay);
        if (!config.getDatatypeFilter().isEmpty()) {
            setting.addOption(BoundedRangeExpansionIterator.DATATYPES_OPT, config.getDatatypeFilterAsString());
        }
        return setting;
    }

    /**
     * Create the scan {@link Range} given a range's lower and upper bounds, along with the end date
     *
     * @param lower
     *            the lower bound
     * @param upper
     *            the upper bound
     * @param endDay
     *            the end date
     * @return the Accumulo scan range
     */
    protected Range createScanRange(String lower, String upper, String endDay) {
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
            log.debug("exception: {}", qe.getMessage());
            throw new IllegalRangeArgumentException(qe);
        }

        if (log.isDebugEnabled()) {
            log.debug("Range: {}", range);
        }
        return range;
    }

    /**
     * Create a {@link CompositeSeekingIterator} IFF the range contains a composite separator
     *
     * @param lower
     *            the lower bound
     * @param upper
     *            the upper bound
     * @return a composite seeking iterator
     */
    protected IteratorSetting createCompositeIterator(String lower, String upper) {
        if (config.getCompositeToFieldMap().get(literalRange.getFieldName()).isEmpty()) {
            return null;
        }

        if (config.getCompositeFieldSeparators() == null || config.getCompositeFieldSeparators().isEmpty()) {
            return null;
        }

        // If this is a composite field, with multiple terms, we need to set up our query to filter based on each component of the composite range
        String compositeSeparator = config.getCompositeFieldSeparators().get(literalRange.getFieldName());
        if (compositeSeparator == null) {
            return null;
        }

        if (!lower.contains(compositeSeparator) && !upper.contains(compositeSeparator)) {
            return null;
        }

        IteratorSetting setting = new IteratorSetting(config.getBaseIteratorPriority() + 51, CompositeSeekingIterator.class);

        setting.addOption(CompositeSeekingIterator.COMPONENT_FIELDS,
                        StringUtils.collectionToCommaDelimitedString(config.getCompositeToFieldMap().get(literalRange.getFieldName())));

        for (String fieldName : config.getCompositeToFieldMap().get(literalRange.getFieldName())) {
            DiscreteIndexType<?> type = config.getFieldToDiscreteIndexTypes().get(fieldName);
            if (type != null)
                setting.addOption(fieldName + CompositeSeekingIterator.DISCRETE_INDEX_TYPE, type.getClass().getName());
        }

        setting.addOption(CompositeSeekingIterator.SEPARATOR, compositeSeparator);
        return setting;
    }

    @Override
    public synchronized IndexLookupMap lookup() {
        await();
        return indexLookupMap;
    }

    /**
     * Wait for the future to complete before returning the {@link IndexLookupMap}
     */
    protected void await() {
        try {
            if (future != null) {
                future.get();
            }
        } catch (Exception e) {
            // any exception causes the range to marked as value exceeded
            handleException(e);
        }
    }

    /**
     * Manipulates the {@link #indexLookupMap} so the bounded range term will be wrapped with an exceeded value marker.
     *
     * @param e
     *            the exception
     */
    protected void handleException(Exception e) {
        log.warn("BoundedRangeIndexLookup saw exception: {}", e.getMessage());
        log.debug("marking bounded range as value exceeded");
        indexLookupMap.put(field, "");
        indexLookupMap.get(field).setThresholdExceeded();
    }
}
