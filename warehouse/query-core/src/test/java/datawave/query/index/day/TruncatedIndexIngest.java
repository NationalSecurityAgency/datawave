package datawave.query.index.day;

import java.util.BitSet;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Ordering;
import com.google.common.collect.TreeMultimap;

import datawave.ingest.table.aggregator.BitSetCombiner;
import datawave.ingest.table.aggregator.util.TruncatedIndexKeyParser;
import datawave.marking.ColumnVisibilityCache;

/**
 * Utility that converts a standard shard index to a truncated index
 */
public class TruncatedIndexIngest extends AbstractIndexIngest {

    private static final Logger log = LoggerFactory.getLogger(TruncatedIndexIngest.class);

    private final TruncatedIndexKeyParser parser = new TruncatedIndexKeyParser();

    public TruncatedIndexIngest() {
        // no-op
    }

    /**
     * Read keys from a standard shard index and write transformed keys to a truncated index
     *
     * @param client
     *            the AccumuloClient
     * @param auths
     *            the Authorizations
     * @param sourceTable
     *            the source table
     * @param destinationTable
     *            the destination table
     */
    @Override
    public void convert(AccumuloClient client, Authorizations auths, String sourceTable, String destinationTable) {

        configureDestination(client, destinationTable);
        Comparator<Key> comparator = (left, right) -> left.compareTo(right, PartialKey.ROW_COLFAM_COLQUAL_COLVIS);
        TreeMultimap<Key,Value> multimap = TreeMultimap.create(comparator, Ordering.natural());

        // transform shard index keys into year index keys
        try (var scanner = client.createScanner(sourceTable, auths)) {
            for (Map.Entry<Key,Value> entry : scanner) {
                parser.parse(entry.getKey());
                Key next = parser.convert();
                BitSet bitset = parser.getBitset();
                multimap.put(next, new Value(bitset.toByteArray()));
            }
        } catch (TableNotFoundException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }

        // simulate a compaction
        BitSetCombiner combiner = new BitSetCombiner();
        TreeMultimap<Key,Value> combined = TreeMultimap.create(comparator, Ordering.natural());
        for (Key key : multimap.keySet()) {
            Collection<Value> bitsets = multimap.get(key);
            Value value = combiner.reduce(key, bitsets.iterator());
            combined.put(key, value);
        }

        try (var bw = client.createBatchWriter(destinationTable)) {
            for (Map.Entry<Key,Value> entry : combined.entries()) {
                Key k = entry.getKey();
                Mutation m = new Mutation(k.getRow());
                ColumnVisibility cv = ColumnVisibilityCache.get(k.getColumnVisibilityData());
                m.put(k.getColumnFamily().toString(), k.getColumnQualifier().toString(), cv, k.getTimestamp(), entry.getValue());
                bw.addMutation(m);
            }
        } catch (TableNotFoundException | MutationsRejectedException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Create and configure a truncated index
     *
     * @param client
     *            the AccumuloClient
     * @param table
     *            the table name
     */
    @Override
    protected void configureDestination(AccumuloClient client, String table) {
        try {
            TableOperations tops = client.tableOperations();
            tops.create(table);
        } catch (AccumuloException | AccumuloSecurityException | TableExistsException e) {
            log.error("Could not create {}", table);
        }
    }

}
