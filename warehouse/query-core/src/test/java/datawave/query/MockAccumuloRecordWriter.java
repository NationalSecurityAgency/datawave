package datawave.query;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;

/**
 * Extracted from several tests
 */
public class MockAccumuloRecordWriter extends RecordWriter<Text,Mutation> {
    private static final Logger log = Logger.getLogger(MockAccumuloRecordWriter.class);
    
    private final HashMap<Text,BatchWriter> writerMap = new HashMap<>();
    
    @Override
    public void write(Text key, Mutation value) throws IOException, InterruptedException {
        try {
            for (ColumnUpdate update : value.getUpdates()) {
                log.debug("Table: "
                                + key
                                + ", Key: "
                                + new Key(value.getRow(), update.getColumnFamily(), update.getColumnQualifier(), update.getColumnVisibility(), update
                                                .getTimestamp()));
            }
            if (writerMap.get(key) == null) {
                throw new NullPointerException("Can't write mutation: No entry in writerMap for table '" + key + "'");
            }
            writerMap.get(key).addMutation(value);
        } catch (MutationsRejectedException e) {
            throw new IOException("Error adding mutation", e);
        }
    }
    
    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        try {
            for (BatchWriter w : writerMap.values()) {
                w.flush();
                w.close();
            }
        } catch (MutationsRejectedException e) {
            throw new IOException("Error closing Batch Writer", e);
        }
    }
    
    public void addWriter(Text tableName, BatchWriter batchWriter) {
        this.writerMap.put(tableName, batchWriter);
    }
}
