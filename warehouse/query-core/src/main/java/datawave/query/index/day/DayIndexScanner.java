package datawave.query.index.day;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

import com.google.common.collect.Multimap;

/**
 * Scans the Day Index and returns a map of node strings to bit sets
 */
public class DayIndexScanner {

    private final String indexTableName;
    private final Authorizations auths;
    private final AccumuloClient client;
    private final Multimap<String,String> valuesAndFields;

    private final BitSetIndexEntrySerializer serDe = new BitSetIndexEntrySerializer();

    public DayIndexScanner(DayIndexConfig config) {
        indexTableName = config.getDayIndexTableName();
        auths = config.getAuths().iterator().next();
        client = config.getClient();
        valuesAndFields = config.getValuesAndFields();
    }

    public BitSetIndexEntry scan(String row) {

        // scan the thing
        try {
            Scanner scanner = client.createScanner(indexTableName, auths);
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

            return null;
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public Set<BitSetIndexEntry> scan(Set<String> rows) {

        // scan the thing
        try {
            BatchScanner scanner = client.createBatchScanner(indexTableName, auths, 0);

            Set<Range> ranges = new HashSet<>();
            for (String row : rows) {
                ranges.add(Range.exact(row));
            }
            scanner.setRanges(ranges);

            for (String field : valuesAndFields.values()) {
                scanner.fetchColumnFamily(new Text(field));
            }

            IteratorSetting setting = new IteratorSetting(30, "DayIndexEntryIterator", DayIndexEntryIterator.class);
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
