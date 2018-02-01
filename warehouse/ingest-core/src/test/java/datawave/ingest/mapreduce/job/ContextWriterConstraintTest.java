package datawave.ingest.mapreduce.job;

import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import datawave.ingest.mapreduce.job.writer.AbstractContextWriter;
import datawave.ingest.mapreduce.job.writer.BulkContextWriter;
import datawave.ingest.mapreduce.job.writer.LiveContextWriter;
import datawave.ingest.mapreduce.StandaloneTaskAttemptContext;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import static datawave.ingest.mapreduce.job.ConstraintChecker.INITIALIZERS;

/**
 * Tests verifying that Constraints are used properly within both BulkContextWriter and LiveContextWriter.
 */
public class ContextWriterConstraintTest {
    
    private Text eventTable = new Text("eventTable");
    private Text indexTable = new Text("indexTable");
    
    private ColumnVisibility goodVis = new ColumnVisibility("A&B");
    private ColumnVisibility badVis = new ColumnVisibility();
    
    private BulkIngestKey goodKey = new BulkIngestKey(eventTable, new Key("row", "fam", "qual", goodVis, 0L));
    private BulkIngestKey badKey = new BulkIngestKey(eventTable, new Key("row", "fam", "qual", badVis, 0L));
    private BulkIngestKey indexBadKey = new BulkIngestKey(indexTable, new Key("row", "fam", "qual", badVis, 0L));
    
    private Value value = new Value();
    
    private StandaloneTaskAttemptContext<?,?,BulkIngestKey,Value> bulkContext;
    private StandaloneTaskAttemptContext<?,?,Text,Mutation> liveContext;
    
    private BulkContextWriter bulkContextWriter;
    private LiveContextWriter liveContextWriter;
    
    @Before
    public void setup() throws Exception {
        Configuration conf = new Configuration();
        conf.setInt(AbstractContextWriter.CONTEXT_WRITER_MAX_CACHE_SIZE, 4);
        conf.set(INITIALIZERS, NonemptyVisibilityConstraint.Initializer.class.getName());
        conf.set(NonemptyVisibilityConstraint.Initializer.TABLE_CONFIG, "eventTable");
        
        bulkContext = new StandaloneTaskAttemptContext<>(conf, null);
        liveContext = new StandaloneTaskAttemptContext<>(conf, null);
        
        bulkContextWriter = new BulkContextWriter();
        bulkContextWriter.setup(conf, false);
        
        liveContextWriter = new LiveContextWriter();
        liveContextWriter.setup(conf, false);
    }
    
    @Test(expected = ConstraintChecker.ConstraintViolationException.class)
    public void shouldFailBulkIngestOnConstraintViolation() throws Exception {
        bulkContextWriter.write(badKey, value, bulkContext);
    }
    
    @Test(expected = ConstraintChecker.ConstraintViolationException.class)
    public void shouldFailBulkIngestOnConstraintViolationDuringMultiWrite() throws Exception {
        Multimap<BulkIngestKey,Value> pairs = TreeMultimap.create();
        pairs.put(badKey, value);
        
        bulkContextWriter.write(pairs, bulkContext);
    }
    
    @Test
    public void shouldPassBulkIngestOnConstraintSatisfied() throws Exception {
        bulkContextWriter.write(goodKey, value, bulkContext);
    }
    
    @Test
    public void shouldPassBulkIngestOnNonconfiguredTables() throws Exception {
        bulkContextWriter.write(indexBadKey, value, bulkContext);
    }
    
    @Test(expected = ConstraintChecker.ConstraintViolationException.class)
    public void shouldFailLiveIngestOnConstraintViolation() throws Exception {
        liveContextWriter.write(badKey, value, liveContext);
    }
    
    @Test(expected = ConstraintChecker.ConstraintViolationException.class)
    public void shouldFailLiveIngestOnConstraintViolationDuringMultiWrite() throws Exception {
        Multimap<BulkIngestKey,Value> pairs = TreeMultimap.create();
        pairs.put(badKey, value);
        
        liveContextWriter.write(pairs, liveContext);
    }
    
    @Test
    public void shouldPassLiveIngestOnConstraintSatisfied() throws Exception {
        liveContextWriter.write(goodKey, value, liveContext);
    }
    
    @Test
    public void shouldPassLiveIngestOnNonconfiguredTables() throws Exception {
        liveContextWriter.write(indexBadKey, value, liveContext);
    }
}
