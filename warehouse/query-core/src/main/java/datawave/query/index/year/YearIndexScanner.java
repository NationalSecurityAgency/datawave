package datawave.query.index.year;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

import com.google.common.collect.Multimap;

import datawave.query.index.day.BitSetIndexEntry;
import datawave.query.index.day.BitSetIndexEntrySerializer;
import datawave.query.index.day.DayIndexConfig;
import datawave.query.index.day.DayIndexEntryIterator;
import datawave.scan.ExecutionHintHelper;
import datawave.scan.ScannerBuilder;

/**
 * Scans the Year Index and returns a map of node strings to bit sets
 */
public class YearIndexScanner {

    private final String indexTableName;
    private final Authorizations auths;
    private final AccumuloClient client;
    private final Map<String,ScannerBase.ConsistencyLevel> consistencyLevels;
    private final Map<String,Map<String,String>> tableHints;

    private final Multimap<String,String> valuesAndFields;

    private final BitSetIndexEntrySerializer serDe = new BitSetIndexEntrySerializer();

    public YearIndexScanner(DayIndexConfig config) {
        indexTableName = config.getYearIndexTableName();
        auths = config.getAuths().iterator().next();
        client = config.getClient();
        consistencyLevels = config.getConsistencyLevels();
        tableHints = config.getTableHints();
        valuesAndFields = config.getValuesAndFields();
    }

    public BitSetIndexEntry scan(String row) {

        //  @formatter:off
        ScannerBuilder builder = ScannerBuilder.create(client)
                .setTableName(indexTableName)
                .setAuthorizations(auths);
        //  @formatter:on

        ScannerBase.ConsistencyLevel consistencyLevel = consistencyLevels.get(indexTableName);
        if (consistencyLevel != null) {
            builder.setConsistencyLevel(consistencyLevel);
        }

        Map<String,String> hints = ExecutionHintHelper.getExecutionHints(indexTableName, tableHints);
        if (hints != null) {
            builder.setScanType(ExecutionHintHelper.getScanType(hints));
            builder.setScanPriority(ExecutionHintHelper.getPriority(hints));
        }

        // scan the thing
        try (Scanner scanner = builder.build()) {
            scanner.setRange(Range.exact(row));

            for (String field : valuesAndFields.values()) {
                scanner.fetchColumnFamily(new Text(field));
            }

            IteratorSetting setting = new IteratorSetting(30, DayIndexEntryIterator.class.getSimpleName(), DayIndexEntryIterator.class);
            setting.addOption(DayIndexEntryIterator.VALUES_AND_FIELDS, DayIndexEntryIterator.mapToString(valuesAndFields));
            scanner.addScanIterator(setting);

            for (Map.Entry<Key,Value> entry : scanner) {
                // should only have one entry per tablet
                return serDe.deserialize(entry.getValue().get());
            }
        }

        return null;
    }

    public Set<BitSetIndexEntry> scan(Set<String> rows) {

        // scan the thing
        try {
            int threads = Math.max(rows.size(), 10);
            BatchScanner scanner = client.createBatchScanner(indexTableName, auths, threads);

            Set<Range> ranges = new HashSet<>();
            for (String row : rows) {
                ranges.add(Range.exact(row));
            }
            scanner.setRanges(ranges);

            for (String field : valuesAndFields.values()) {
                scanner.fetchColumnFamily(new Text(field));
            }

            IteratorSetting setting = new IteratorSetting(30, DayIndexEntryIterator.class.getSimpleName(), DayIndexEntryIterator.class);
            setting.addOption(DayIndexEntryIterator.VALUES_AND_FIELDS, DayIndexEntryIterator.mapToString(valuesAndFields));
            scanner.addScanIterator(setting);

            Set<BitSetIndexEntry> results = new HashSet<>();

            for (Map.Entry<Key,Value> entry : scanner) {
                BitSetIndexEntry indexEntry = serDe.deserialize(entry.getValue().get());
                results.add(indexEntry);
            }
            return results;
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

}
