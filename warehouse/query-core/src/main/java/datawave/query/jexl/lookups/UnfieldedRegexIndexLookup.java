package datawave.query.jexl.lookups;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;

import datawave.core.iterators.TimeoutExceptionIterator;
import datawave.core.iterators.UnfieldedRegexExpansionIterator;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.tables.ScannerFactory;
import datawave.util.time.DateHelper;

/**
 * An asynchronous index lookup which expands a fielded regex into discrete fields and values.
 * <p>
 * Because an unfielded term is not executable it is best if this index lookup runs without a time or field threshold.
 */
public class UnfieldedRegexIndexLookup extends BaseRegexIndexLookup {

    private static final Logger log = LoggerFactory.getLogger(UnfieldedRegexIndexLookup.class);

    private final Set<String> fields;

    public UnfieldedRegexIndexLookup(ShardQueryConfiguration config, ScannerFactory scannerFactory, ExecutorService execService, String pattern, Range range,
                    boolean reverse, Set<String> fields) {
        super(config, scannerFactory, true, execService, pattern, range, reverse);
        this.fields = Objects.requireNonNullElse(fields, Collections.emptySet());
        log.info("Created UnfieldedRegexIndexLookup with pattern {}", pattern);
    }

    @Override
    public void submit() {
        if (indexLookupMap == null) {
            indexLookupMap = new IndexLookupMap(Integer.MAX_VALUE, Integer.MAX_VALUE);

            execService.submit(() -> {
                String tableName = reverse ? config.getReverseIndexTableName() : config.getIndexTableName();
                try (var scanner = config.getClient().createScanner(tableName, config.getAuthorizations().iterator().next())) {
                    String hintKey = getHintKey(tableName);
                    scanner.setExecutionHints(Map.of(tableName, hintKey));

                    // use a different timeout threshold for unfielded expansions
                    IteratorSetting timeoutIterator = createTimeoutIterator();
                    scanner.addScanIterator(timeoutIterator);

                    IteratorSetting regexIterator = createRegexIterator();
                    scanner.addScanIterator(regexIterator);

                    IteratorSetting timeoutExceptionIterator = createTimeoutExceptionIterator();
                    scanner.addScanIterator(timeoutExceptionIterator);

                    scanner.setRange(range);

                    for (String field : fields) {
                        scanner.fetchColumnFamily(new Text(field));
                    }

                    for (Map.Entry<Key,Value> entry : scanner) {
                        Key key = entry.getKey();

                        if (TimeoutExceptionIterator.exceededTimedValue(entry)) {
                            indexLookupMap.setTimeoutExceeded(true);
                            break;
                        }

                        String value = key.getRow().toString();
                        String field = key.getColumnFamily().toString();
                        if (reverse) {
                            value = reverse(value);
                        }
                        indexLookupMap.put(field, value);
                    }

                } catch (Exception e) {
                    indexLookupMap.setExceptionSeen(true);
                    log.error(e.getMessage(), e);
                } finally {
                    latch.countDown();
                }
            });
        }
    }

    @Override
    protected IteratorSetting createRegexIterator() {
        IteratorSetting setting = new IteratorSetting(config.getBaseIteratorPriority() + 50, "unfielded regex expansion",
                        UnfieldedRegexExpansionIterator.class.getName());
        setting.addOption(UnfieldedRegexExpansionIterator.PATTERN, pattern);
        setting.addOption(UnfieldedRegexExpansionIterator.START_DATE, DateHelper.format(config.getBeginDate()));
        setting.addOption(UnfieldedRegexExpansionIterator.END_DATE, DateHelper.format(config.getEndDate()));
        setting.addOption(UnfieldedRegexExpansionIterator.REVERSE, Boolean.toString(reverse));
        if (!config.getDatatypeFilter().isEmpty()) {
            setting.addOption(UnfieldedRegexExpansionIterator.DATATYPES, Joiner.on(',').join(config.getDatatypeFilter()));
        }
        return setting;
    }

    @Override
    public IndexLookupMap lookup() {
        await();
        return indexLookupMap;
    }
}
