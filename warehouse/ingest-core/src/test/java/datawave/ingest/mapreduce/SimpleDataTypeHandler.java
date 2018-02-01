package datawave.ingest.mapreduce;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.ingest.IngestHelperInterface;
import datawave.ingest.mapreduce.handler.DataTypeHandler;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.metadata.RawRecordMetadata;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * A {@link DataTypeHandler} used for unit testing.
 * <p/>
 * The processBulk method will generate mutations with the following format:
 * 
 * <pre>
 *     table = 'accumulo_table'
 *     row = date (yyyyMMdd)
 *     fam = field name
 *     qual = field value
 *     val = 1
 * </pre>
 */
public class SimpleDataTypeHandler<IK> implements DataTypeHandler<IK> {
    
    public static Text TABLE = new Text("accumulo_table");
    
    private DateFormat df = new SimpleDateFormat("yyyyMMdd");
    
    @Override
    public void setup(TaskAttemptContext context) {
        
    }
    
    @Override
    public String[] getTableNames(Configuration conf) {
        return new String[0];
    }
    
    @Override
    public int[] getTableLoaderPriorities(Configuration conf) {
        return new int[0];
    }
    
    @Override
    public Multimap<BulkIngestKey,Value> processBulk(IK key, RawRecordContainer event, Multimap<String,NormalizedContentInterface> fields,
                    StatusReporter reporter) {
        String date = df.format(new Date());
        Text table = new Text(TABLE);
        Value value = new Value("1".getBytes());
        
        Multimap<BulkIngestKey,Value> pairs = HashMultimap.create();
        for (Map.Entry<String,NormalizedContentInterface> entry : fields.entries()) {
            BulkIngestKey bik = new BulkIngestKey(table, new Key(date, entry.getKey(), entry.getValue().getEventFieldValue()));
            pairs.put(bik, value);
        }
        
        return pairs;
    }
    
    @Override
    public IngestHelperInterface getHelper(Type datatype) {
        return SimpleDataTypeHelper.create();
    }
    
    @Override
    public void close(TaskAttemptContext context) {
        
    }
    
    @Override
    public RawRecordMetadata getMetadata() {
        return null;
    }
}
