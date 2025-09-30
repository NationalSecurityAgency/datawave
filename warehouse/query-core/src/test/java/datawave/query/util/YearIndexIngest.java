package datawave.query.util;

import java.util.BitSet;
import java.util.Calendar;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.TimeZone;

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
import datawave.util.time.DateHelper;

/**
 * Utility that converts a shard index to a shard year index table
 */
public class YearIndexIngest {

    private final static Logger log = LoggerFactory.getLogger(YearIndexIngest.class);

    private final ShardIndexKey parser = new ShardIndexKey();

    public YearIndexIngest() {

    }

    /**
     * The in memory version of {@link TableOperations} does not actually execute a compaction. This method will transform the shard index keys into a year
     * index key format and simulate a compaction to combine the bitsets.
     *
     * @param client
     *            the AccumuloClient
     * @param auths
     *            the Authorizations
     * @param shardIndexTableName
     *            the shard index table name
     * @param yearIndexTableName
     *            the year index table name
     */
    public void convertToYearIndex(AccumuloClient client, Authorizations auths, String shardIndexTableName, String yearIndexTableName) {
        createAndConfigureYearIndex(client, yearIndexTableName);

        Comparator<Key> comparator = (left, right) -> left.compareTo(right, PartialKey.ROW_COLFAM_COLQUAL_COLVIS);

        TreeMultimap<Key,Value> multimap = TreeMultimap.create(comparator, Ordering.natural());

        // transform shard index keys into year index keys
        try (Scanner scanner = client.createScanner(shardIndexTableName, auths)) {
            for (Map.Entry<Key,Value> entry : scanner) {
                Key next = transform(entry.getKey());
                int day = dayOfYearFromShard(parser.getShard());
                BitSet bits = new BitSet();
                bits.set(day);
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
        try (BatchWriter bw = client.createBatchWriter(yearIndexTableName)) {
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

    private void createAndConfigureYearIndex(AccumuloClient client, String yearIndexTableName) {
        try {
            TableOperations tops = client.tableOperations();

            tops.create(yearIndexTableName);

            // TODO -- set combiner, add splits, compact

        } catch (AccumuloException | AccumuloSecurityException | TableExistsException e) {
            log.error("Could not create {}", yearIndexTableName);
        }
    }

    /**
     * Grab the year from the shard
     *
     * @param shard
     *            the shard
     * @return the year portion
     */
    private String rowFromShard(String shard) {
        int index = shard.indexOf('_');
        index -= 4; // trim out
        return shard.substring(0, index);
    }

    /**
     * Get the day of the year from the shard
     *
     * @param shard
     *            the shard
     * @return the day of the year
     */
    private int dayOfYearFromShard(String shard) {
        int index = shard.indexOf('_');
        String date = shard.substring(0, index);
        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
        calendar.setTime(DateHelper.parse(date));

        int dayOfYear = calendar.get(Calendar.DAY_OF_YEAR);
        log.trace("day of year: {}", dayOfYear);
        return dayOfYear;
    }

}
