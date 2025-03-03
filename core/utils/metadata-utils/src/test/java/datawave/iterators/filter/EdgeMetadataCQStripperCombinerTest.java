package datawave.iterators.filter;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.protobuf.InvalidProtocolBufferException;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.metadata.protobuf.EdgeMetadata.MetadataValue;
import datawave.metadata.protobuf.EdgeMetadata.MetadataValue.Metadata;
import datawave.util.time.DateHelper;

public class EdgeMetadataCQStripperCombinerTest {
    private static final Logger log = Logger.getLogger(EdgeMetadataCQStripperCombinerTest.class);
    private AccumuloClient accumuloClient;
    private static String EDGE_TABLE_NAME = "DatawaveMetadata";
    private static String ROW_VISIBILITY = "IN_VISIBLE";
    
    private static long startTime = 40L * (365L * 24L * 60L * 60L * 1000L); // set intitial time to be 40 years after the epoch
    private static long day = (24L * 60L * 60L * 1000L);
    private static int numAttributeEntries = 6;
    private static int numCondensedEntries = 8; // for testing sanity make this an even number
    
    @BeforeEach
    public void init() throws Exception {
        
        InMemoryInstance i = new InMemoryInstance(EdgeMetadataCQStripperCombinerTest.class.toString());
        accumuloClient = new InMemoryAccumuloClient("root", i);
        
        // Create a table
        accumuloClient.tableOperations().create(EDGE_TABLE_NAME);
        
        // Create a BatchWriter to the table
        BatchWriter recordWriter = accumuloClient.createBatchWriter(EDGE_TABLE_NAME, new BatchWriterConfig());
        
        loadSomeData(recordWriter);
        
        // Print out rows to make sure they look like what you expect
        printEdgeTable(null);
    }
    
    @AfterEach
    public void tearDown() {
        accumuloClient = null;
    }
    
    private static Value getMetadata(int i) {
        Metadata.Builder builder = Metadata.newBuilder();
        MetadataValue.Builder mBuilder = MetadataValue.newBuilder();
        
        mBuilder.clearMetadata();
        builder.clear();
        
        builder.setSink("Sink_" + i);
        builder.setSource("Source_" + i);
        builder.setDate(DateHelper.format(new Date()));
        
        mBuilder.addMetadata(builder);
        
        return new Value(mBuilder.build().toByteArray());
    }
    
    private static void loadSomeData(BatchWriter writer) throws MutationsRejectedException {
        Mutation m;
        ColumnVisibility vis = new ColumnVisibility(ROW_VISIBILITY);
        
        long ts;
        
        for (int i = 0; i < numCondensedEntries; i++) {
            
            ts = startTime;
            for (int j = 0; j < numAttributeEntries; j++) {
                m = new Mutation("type" + i + "/relationship" + i);
                String columnFamily = "edge";
                String columnQualifier = "attribute1_" + (j % 2) + "/attribute2_" + j + "/attribute3_" + j;
                Value value = getMetadata(i + j);
                
                m.put(columnFamily, columnQualifier, vis, ts, value);
                writer.addMutation(m);
                
                ts += day * 2L; // increment by 2 days
            }
        }
        
        writer.close();
        
    }
    
    @Test
    public void testReduction() throws Exception {
        
        IteratorSetting cqStripingSetting = new IteratorSetting(18, EdgeMetadataCQStrippingIterator.class.getSimpleName() + "_" + 18,
                        EdgeMetadataCQStrippingIterator.class);
        
        IteratorSetting combinerSetting = new IteratorSetting(19, EdgeMetadataCQStripperCombiner.class.getSimpleName() + "_" + 19,
                        EdgeMetadataCQStripperCombiner.class);
        combinerSetting.addOption("columns", "edge");
        
        Scanner scan = accumuloClient.createScanner(EDGE_TABLE_NAME, new Authorizations(ROW_VISIBILITY));
        
        scan.addScanIterator(cqStripingSetting);
        scan.addScanIterator(combinerSetting);
        
        int counter = 0;
        for (Map.Entry<Key,Value> entry : scan) {
            
            if (counter % 2 == 0) {
                assertEquals((startTime + day * 8L), entry.getKey().getTimestamp(), "Wrong timestamp");
            } else {
                assertEquals((startTime + day * 10L), entry.getKey().getTimestamp(), "Wrong timestamp");
            }
            
            MetadataValue meta = MetadataValue.parseFrom(entry.getValue().get());
            
            assertEquals((numAttributeEntries / 2), meta.getMetadataCount(), "Unexpected amount of metadata.");
            counter++;
        }
        
        List<IteratorSetting> iterators = new ArrayList<>();
        iterators.add(cqStripingSetting);
        iterators.add(combinerSetting);
        printEdgeTable(iterators);
    }
    
    private void printEdgeTable(List<IteratorSetting> iterators) throws Exception {
        Scanner scan = accumuloClient.createScanner(EDGE_TABLE_NAME, new Authorizations(ROW_VISIBILITY));
        
        if (iterators != null) {
            for (IteratorSetting iteratorSetting : iterators) {
                scan.addScanIterator(iteratorSetting);
            }
        }
        
        for (Map.Entry<Key,Value> entry : scan) {
            String row = entry.getKey().getRow().toString();
            String columnFamily = entry.getKey().getColumnFamily().toString();
            String columnQualifier = entry.getKey().getColumnQualifier().toString();
            String visibility = entry.getKey().getColumnVisibility().toString();
            long timeStamp = entry.getKey().getTimestamp();
            Value value = entry.getValue();
            
            MetadataValue meta;
            try {
                meta = MetadataValue.parseFrom(value.get());
            } catch (InvalidProtocolBufferException e) {
                log.error("Found invalid Edge Metadata Value bytes.", e);
                continue;
            }
            
            System.out.println(row + " " + columnFamily + ":" + columnQualifier + " " + "[" + visibility + "] " + timeStamp + " "
                            + meta.toString().replace('\n', ' '));
        }
        
    }
}
