package datawave.ingest.wikipedia;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

/**
 *
 */
public class DocWriter implements Runnable {
    private static final Logger log = Logger.getLogger(DocWriter.class);

    Key k;
    byte[] shardId;
    byte[] visibility;
    Value value;
    BatchWriter docWriter;

    public DocWriter(BatchWriter docWriter) {
        this.docWriter = docWriter;
    }

    @Override
    public void run() {
        log.debug("Writing out a document of size " + value.get().length + " bytes.");
        Mutation m = new Mutation(new Text(shardId));
        m.put(k.getColumnFamily(), k.getColumnQualifier(), new ColumnVisibility(visibility), k.getTimestamp(), value);
        try {
            docWriter.addMutation(m);
        } catch (MutationsRejectedException e) {
            log.error("Could not write document payload to Accumulo!", e);
        }
    }
}
