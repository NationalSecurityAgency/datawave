package datawave.query.util;

import java.util.BitSet;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Ordering;
import com.google.common.collect.TreeMultimap;

import datawave.ingest.table.aggregator.BitSetCombiner;
import datawave.query.data.parsers.ShardIndexKey;

/**
 * Utility that converts a shard index to a shard day index table
 */
public class DayIndexIngest {

    private final static Logger log = LoggerFactory.getLogger(DayIndexIngest.class);

    private final ShardIndexKey parser = new ShardIndexKey();

    public DayIndexIngest() {

    }

    public void convertToDayIndex(AccumuloClient client, Authorizations auths, String shardIndexTableName, String dayIndexTableName) {
        createAndConfigureDayIndex(client, dayIndexTableName);

        Comparator<Key> comparator = (left, right) -> left.compareTo(right, PartialKey.ROW_COLFAM_COLQUAL_COLVIS);

        TreeMultimap<Key,Value> multimap = TreeMultimap.create(comparator, Ordering.natural());

        // transform shard index keys into year index keys
        try (Scanner scanner = client.createScanner(shardIndexTableName, auths)) {
            for (Map.Entry<Key,Value> entry : scanner) {
                Key next = transform(entry.getKey());
                int shard = numFromShard(parser.getShard());
                BitSet bits = new BitSet();
                bits.set(shard);
                multimap.put(next, new Value(bits.toByteArray()));
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

        // write keys and values as mutations
        try (BatchWriter bw = client.createBatchWriter(dayIndexTableName)) {
            for (Map.Entry<Key,Value> entry : combined.entries()) {
                Key key = entry.getKey();
                Mutation m = new Mutation(key.getRow());
                m.put(key.getColumnFamily().toString(), key.getColumnQualifier().toString(), key.getColumnVisibilityParsed(), key.getTimestamp(),
                                entry.getValue());
                bw.addMutation(m);
            }
        } catch (TableNotFoundException | MutationsRejectedException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    private void createAndConfigureDayIndex(AccumuloClient client, String dayIndexTableName) {

        try {
            TableOperations tops = client.tableOperations();

            tops.create(dayIndexTableName);

            // TODO -- set combiner
            // tops.setProperty();

        } catch (AccumuloException | AccumuloSecurityException | TableExistsException e) {
            log.error("Could not create {}", dayIndexTableName);
        }
    }

    /**
     * Transform a shard index key to a year index key
     *
     * @param key
     *            the shard index key
     * @return a year index key
     */
    protected Key transform(Key key) {
        parser.parse(key);
        String row = rowFromShard(parser.getShard()) + '\u0000' + parser.getValue();
        String cf = parser.getField();
        String cq = parser.getDatatype();
        return new Key(row, cf, cq, key.getColumnVisibilityParsed(), key.getTimestamp());
    }

    private String rowFromShard(String shard) {
        int index = shard.indexOf('_');
        return shard.substring(0, index);
    }

    private int numFromShard(String shard) {
        int index = shard.indexOf('_');
        String shardNumber = shard.substring(index + 1);
        log.trace("shard number {}", shardNumber);
        return Integer.parseInt(shardNumber);
    }

}
