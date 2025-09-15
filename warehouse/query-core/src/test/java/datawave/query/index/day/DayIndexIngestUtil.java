package datawave.query.index.day;

import java.util.BitSet;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.util.TableName;

/**
 * Test util for ingesting day index data
 */
public class DayIndexIngestUtil {

    private static final String DAY_INDEX_TABLE = TableName.SHARD_DAY_INDEX;

    private final AccumuloClient client;

    private final Set<String> indexedFields = Set.of("FIELD_A", "FIELD_B", "FIELD_C");

    public DayIndexIngestUtil(InMemoryInstance instance) throws Exception {
        client = new InMemoryAccumuloClient("user", instance);
        client.tableOperations().create(DAY_INDEX_TABLE);
    }

    public AccumuloClient getClient() {
        return client;
    }

    public void writeData() throws Exception {
        try (BatchWriter bw = client.createBatchWriter(DAY_INDEX_TABLE)) {
            List<String> days = List.of("20240101", "20240102", "20240103", "20240104", "20240105");
            for (String day : days) {
                // FIELD_A and FIELD_B are exclusive datatypes
                write(bw, "FIELD_A", "even", "datatype-a", day, getEven());
                write(bw, "FIELD_A", "odd", "datatype-a", day, getOdd());
                write(bw, "FIELD_A", "prime", "datatype-a", day, getPrime());
                write(bw, "FIELD_A", "three", "datatype-a", day, getThree());

                write(bw, "FIELD_B", "even", "datatype-b", day, getEven());
                write(bw, "FIELD_B", "odd", "datatype-b", day, getOdd());
                write(bw, "FIELD_B", "prime", "datatype-b", day, getPrime());
                write(bw, "FIELD_B", "three", "datatype-b", day, getThree());

                // 'merged' value has different bitsets for the same value
                write(bw, "FIELD_A", "merged", "datatype-a", day, getEven());
                write(bw, "FIELD_B", "merged", "datatype-b", day, getOdd());
                write(bw, "FIELD_C", "merged", "datatype-c", day, getPrime());
            }
        }
    }

    private static void write(BatchWriter bw, String field, String value, String datatype, String date, BitSet bits) throws Exception {
        Mutation m = new Mutation(date + '\u0000' + value);
        m.put(field, datatype, serializeBitSet(bits));
        bw.addMutation(m);
    }

    public BitSet getEven() {
        return BitSetFactory.create(0, 2, 4, 6, 8);
    }

    public BitSet getOdd() {
        return BitSetFactory.create(1, 3, 5, 7, 9);
    }

    public BitSet getPrime() {
        return BitSetFactory.create(2, 3, 5, 7);
    }

    public BitSet getThree() {
        return BitSetFactory.create(3, 6);
    }

    private static Value serializeBitSet(BitSet bits) {
        return new Value(bits.toByteArray());
    }

    public Set<String> getIndexedFields() {
        return indexedFields;
    }
}
