package datawave.test.framework.writers;

import java.util.List;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;

import datawave.table.constants.TableName;

public class MetadataTableWriter {

    private static final Value EMPTY_VALUE = new Value();

    private MetadataTableWriter() {
        // enforce static access
    }

    public static void write(AccumuloClient client, List<Key> keys) {
        try (BatchWriter bw = client.createBatchWriter(TableName.METADATA)) {
            for (Key key : keys) {
                Mutation m = new Mutation(key.getRow());
                m.put(key.getColumnFamily(), key.getColumnQualifier(), key.getColumnVisibilityParsed(), EMPTY_VALUE);
                bw.addMutation(m);
            }
        } catch (TableNotFoundException | MutationsRejectedException e) {
            throw new RuntimeException(e);
        }
    }
}
