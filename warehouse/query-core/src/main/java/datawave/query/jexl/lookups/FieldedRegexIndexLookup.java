package datawave.query.jexl.lookups;

import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

import datawave.core.iterators.FieldedRegexExpansionIterator;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.tables.ScannerFactory;
import datawave.util.time.DateHelper;

/**
 * An asynchronous index lookup which expands a fielded regex into discrete values.
 * <p>
 * A fielded regex is already executable so this lookup is allowed to hit timeout and value thresholds.
 */
public class FieldedRegexIndexLookup extends BaseRegexIndexLookup {

    private static final Logger log = LoggerFactory.getLogger(FieldedRegexIndexLookup.class);

    private final String field;

    public FieldedRegexIndexLookup(ShardQueryConfiguration config, ScannerFactory scannerFactory, ExecutorService execService, String field, String pattern,
                    Range range, boolean reverse) {
        super(config, scannerFactory, false, execService, pattern, range, reverse);
        this.field = field;

        if (log.isDebugEnabled()) {
            log.debug("Created FieldedRegexIndexLookup with field {} and pattern {}", field, pattern);
        }
    }

    @Override
    public void submit() {
        if (indexLookupMap == null) {
            indexLookupMap = new IndexLookupMap(config.getMaxUnfieldedExpansionThreshold(), config.getMaxValueExpansionThreshold());

            // do not bother running the scan if the timeout threshold is zero
            if (config.getMaxIndexScanTimeMillis() == 0) {
                indexLookupMap.put(field, "");
                indexLookupMap.get(field).setThresholdExceeded();
                return;
            }

            Preconditions.checkNotNull(monitor, "FieldedRegexIndexLookup requires a ScanMonitor");
            Runnable runnable = createRunnable();

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
     */
    protected Runnable createRunnable() {
        return () -> {
            String tableName = getTableName();
            try (Scanner scanner = config.getClient().createScanner(tableName, config.getAuthorizations().iterator().next())) {
                String hintKey = getHintKey(tableName);
                scanner.setExecutionHints(Map.of(tableName, hintKey));

                IteratorSetting regexIterator = createRegexIterator();
                scanner.addScanIterator(regexIterator);

                scanner.setRange(range);

                scanner.fetchColumnFamily(new Text(field));

                for (Map.Entry<Key,Value> entry : scanner) {
                    Key key = entry.getKey();
                    String value = key.getRow().toString();
                    if (reverse) {
                        value = reverse(value);
                    }

                    indexLookupMap.put(field, value);
                }

            } catch (Exception e) {
                handleException(e);
            }
        };
    }

    @Override
    protected IteratorSetting createRegexIterator() {
        IteratorSetting setting = new IteratorSetting(config.getBaseIteratorPriority() + 50, "fielded regex expansion",
                        FieldedRegexExpansionIterator.class.getName());
        setting.addOption(FieldedRegexExpansionIterator.FIELD, field);
        setting.addOption(FieldedRegexExpansionIterator.PATTERN, pattern);
        setting.addOption(FieldedRegexExpansionIterator.START_DATE, DateHelper.format(config.getBeginDate()));
        setting.addOption(FieldedRegexExpansionIterator.END_DATE, DateHelper.format(config.getEndDate()));
        if (!config.getDatatypeFilter().isEmpty()) {
            setting.addOption(FieldedRegexExpansionIterator.DATATYPES, Joiner.on(',').join(config.getDatatypeFilter()));
        }
        setting.addOption(FieldedRegexExpansionIterator.REVERSE, Boolean.toString(reverse));
        return setting;
    }

    @Override
    public IndexLookupMap lookup() {
        await();
        return indexLookupMap;
    }

    /**
     * An exception while expanding a fielded regex retains the field but clears all collected values, if any such values exist.
     * <p>
     * The entry is then marked as threshold exceeded.
     *
     * @param e
     *            the exception
     */
    @Override
    protected void handleException(Exception e) {
        log.warn("FieldedRegexIndexLookup saw exception: {}", e.getMessage());
        log.debug("marking fielded regex as exceeded value");
        indexLookupMap.setExceptionSeen(true);
        indexLookupMap.setTimeoutExceeded(true); // stub this out
        indexLookupMap.put(field, "");
        indexLookupMap.get(field).setThresholdExceeded();
    }
}
