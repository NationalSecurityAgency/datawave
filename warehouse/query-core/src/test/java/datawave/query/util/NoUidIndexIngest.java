package datawave.query.util;

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
import org.apache.accumulo.core.security.ColumnVisibility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Ordering;
import com.google.common.collect.TreeMultimap;
import com.google.protobuf.InvalidProtocolBufferException;

import datawave.ingest.protobuf.Uid;
import datawave.ingest.table.aggregator.KeepCountOnlyNoUidAggregator;
import datawave.marking.ColumnVisibilityCache;
import datawave.query.index.day.AbstractIndexIngest;

/**
 * Utility that converts a standard shard index by dropping all uids
 */
public class NoUidIndexIngest extends AbstractIndexIngest {

    private static final Logger log = LoggerFactory.getLogger(NoUidIndexIngest.class);

    @Override
    protected void configureDestination(AccumuloClient client, String destination) {
        try {
            TableOperations tops = client.tableOperations();
            tops.create(destination);
        } catch (AccumuloException | AccumuloSecurityException | TableExistsException e) {
            log.error("Could not create {}", destination);
        }
    }

    @Override
    public void convert(AccumuloClient client, Authorizations auths, String source, String destination) {
        configureDestination(client, destination);

        Comparator<Key> comparator = (left, right) -> left.compareTo(right, PartialKey.ROW_COLFAM_COLQUAL_COLVIS);

        TreeMultimap<Key,Value> multimap = TreeMultimap.create(comparator, Ordering.natural());

        // transform shard index keys into year index keys
        try (Scanner scanner = client.createScanner(source, auths)) {
            for (Map.Entry<Key,Value> entry : scanner) {
                // key remains the same
                Value value = removeUids(entry.getValue());
                multimap.put(entry.getKey(), value);
            }
        } catch (TableNotFoundException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }

        // simulate a compaction
        KeepCountOnlyNoUidAggregator aggregator = new KeepCountOnlyNoUidAggregator();
        TreeMultimap<Key,Value> combined = TreeMultimap.create(comparator, Ordering.natural());
        for (Key key : multimap.keySet()) {
            Value aggregated = aggregator.reduce(key, multimap.get(key).iterator());
            combined.put(key, aggregated);
        }

        // write keys and values as mutations
        try (BatchWriter bw = client.createBatchWriter(destination)) {
            for (Map.Entry<Key,Value> entry : combined.entries()) {
                Key key = entry.getKey();
                Mutation m = new Mutation(key.getRow());
                ColumnVisibility cv = ColumnVisibilityCache.get(key.getColumnVisibilityData());
                m.put(key.getColumnFamily(), key.getColumnQualifier(), cv, key.getTimestamp(), entry.getValue());
                bw.addMutation(m);
            }
        } catch (TableNotFoundException | MutationsRejectedException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    protected Value removeUids(Value value) {
        try {
            Uid.List list = Uid.List.parseFrom(value.get());
            //  @formatter:off
            Uid.List next = Uid.List.newBuilder()
                    .setCOUNT(list.getCOUNT())
                    .setIGNORE(true)
                    .build();
            //  @formatter:on
            return new Value(next.toByteArray());
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }
}
