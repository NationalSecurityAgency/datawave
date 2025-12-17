package datawave.query.jexl.lookups;

import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;

import datawave.core.iterators.FieldedRegexExpansionIterator;
import datawave.core.iterators.TimeoutExceptionIterator;
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
        log.info("Created FieldedRegexIndexLookup with field {} and pattern {}", field, pattern);
    }

    @Override
    public void submit() {
        if (indexLookupMap == null) {
            indexLookupMap = new IndexLookupMap(config.getMaxUnfieldedExpansionThreshold(), config.getMaxValueExpansionThreshold());

            // do not bother running the scan if the timeout threshold is zero
            if (config.getMaxIndexScanTimeMillis() == 0) {
                indexLookupMap.put(field, "");
                indexLookupMap.get(field).setThresholdExceeded();
                latch.countDown();
                return;
            }

            execService.submit(() -> {
                String tableName = getTableName();
                try (var scanner = config.getClient().createScanner(tableName, config.getAuthorizations().iterator().next())) {
                    String hintKey = getHintKey(tableName);
                    scanner.setExecutionHints(Map.of(tableName, hintKey));

                    IteratorSetting timeoutIterator = createTimeoutIterator();
                    scanner.addScanIterator(timeoutIterator);

                    IteratorSetting regexIterator = createRegexIterator();
                    scanner.addScanIterator(regexIterator);

                    IteratorSetting timeoutExceptionIterator = createTimeoutExceptionIterator();
                    scanner.addScanIterator(timeoutExceptionIterator);

                    scanner.setRange(range);

                    scanner.fetchColumnFamily(new Text(field));

                    for (Map.Entry<Key,Value> entry : scanner) {
                        Key key = entry.getKey();

                        if (TimeoutExceptionIterator.exceededTimedValue(entry)) {
                            exceededTimeoutThreshold.set(true);
                            indexLookupMap.setTimeoutExceeded(true);
                            indexLookupMap.put(field, "");
                            indexLookupMap.get(field).setThresholdExceeded();
                            break;
                        }

                        String value = key.getRow().toString();
                        if (reverse) {
                            value = reverse(value);
                        }

                        indexLookupMap.put(field, value);
                    }

                } catch (ExceededThresholdException e) {
                    log.info("ExceededThresholdException", e);
                    exceededValueThreshold.set(true);
                    indexLookupMap.get(field).setThresholdExceeded();
                } catch (Exception e) {
                    exceptionSeen.set(true);
                    log.error(e.getMessage(), e);
                } finally {
                    latch.countDown();
                }
            });
        }
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
}
