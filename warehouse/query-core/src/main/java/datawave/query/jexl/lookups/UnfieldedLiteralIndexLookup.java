package datawave.query.jexl.lookups;

import static datawave.query.jexl.lookups.ShardIndexQueryTableStaticMethods.EXPANSION_HINT_KEY;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

import datawave.core.iterators.FieldExpansionIterator;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.tables.ScannerFactory;
import datawave.util.time.DateHelper;

/**
 * An {@link IndexLookup} that wraps {@link FieldExpansionIterator}.
 */
public class UnfieldedLiteralIndexLookup extends AsyncIndexLookup {

    private static final Logger log = LoggerFactory.getLogger(UnfieldedLiteralIndexLookup.class);

    protected String term;
    protected Range range;

    private Future<?> future;

    public UnfieldedLiteralIndexLookup(ShardQueryConfiguration config, ScannerFactory scannerFactory, String term, Set<String> fields,
                    ExecutorService execService) {
        super(config, scannerFactory, true, execService);
        this.term = term;
        this.fields = new HashSet<>();
        if (fields != null) {
            this.fields.addAll(fields);
        }
        this.range = getScanRange();
    }

    @Override
    public void submit() {
        if (indexLookupMap == null) {
            indexLookupMap = new IndexLookupMap(config.getMaxUnfieldedExpansionThreshold(), config.getMaxValueExpansionThreshold());

            Preconditions.checkNotNull(monitor, "UnfieldedLiteralIndexLookup requires a ScanMonitor");
            Runnable runnable = createRunnable(getTableName(), config.getAuthorizations().iterator().next());

            future = execService.submit(runnable);
            monitor.registerTask(future, config.getMaxAnyFieldScanTimeMillis());
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
                scanner.setRange(range);

                for (String field : fields) {
                    scanner.fetchColumnFamily(new Text(field));
                }

                IteratorSetting setting = createIteratorSetting();
                scanner.addScanIterator(setting);

                for (Map.Entry<Key,Value> entry : scanner) {
                    Key key = entry.getKey();
                    if (log.isTraceEnabled()) {
                        log.trace("tk: {}", key.toStringNoTime());
                    }
                    String field = key.getColumnFamily().toString();
                    String value = key.getRow().toString();
                    indexLookupMap.put(field, value);
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
            setting.addOption(FieldExpansionIterator.DATATYPES, config.getDatatypeFilterAsString());
        }

        if (!fields.isEmpty()) {
            setting.addOption(FieldExpansionIterator.FIELDS, Joiner.on(',').join(fields));
        }

        return setting;
    }

    @Override
    public IndexLookupMap lookup() {
        await();
        return indexLookupMap;
    }

    protected void await() {
        try {
            if (future != null) {
                future.get();
            }
        } catch (Exception e) {
            handleException(e);
        }
    }

    /**
     * Any exception indicates a failure to expand the unfielded literal and should fail the query.
     * <p>
     * A {@link DatawaveFatalQueryException} is thrown instead of a raw exception.
     *
     * @param e
     *            the exception
     */
    protected void handleException(Exception e) {
        log.warn("UnfieldedLiteralIndexLookup saw exception: {}", e.getMessage());
        log.debug("unfielded literal expansion failed, failing the query");
        throw new DatawaveFatalQueryException("Failed to expand unfielded literal");
    }
}
