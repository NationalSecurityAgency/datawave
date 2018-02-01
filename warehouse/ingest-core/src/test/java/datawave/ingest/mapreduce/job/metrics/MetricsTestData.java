package datawave.ingest.mapreduce.job.metrics;

import org.apache.accumulo.core.data.Value;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.config.BaseNormalizedContent;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import org.apache.hadoop.conf.Configuration;
import org.easymock.EasyMock;

import java.util.Map;

/**
 * Utilites for creating common testing data.
 */
public class MetricsTestData {
    
    private static final String DEFAULT_CONFIG = "/datawave/ingest/mapreduce/job/metrics/test-metrics-config.xml";
    
    /**
     * Loads the test config xml file.
     *
     * @return conf
     */
    public static Configuration loadDefaultTestConfig() {
        Configuration conf = new Configuration();
        conf.addResource(MetricsTestData.class.getResourceAsStream(DEFAULT_CONFIG));
        return conf;
    }
    
    /**
     * Creates a multimap of fields from a vararg of Strings. Requires key, value pairs.
     *
     * @param pairs
     * @return fields
     */
    public static Multimap<String,NormalizedContentInterface> createFields(String... pairs) {
        assert pairs.length % 2 == 0;
        
        Multimap<String,NormalizedContentInterface> fields = HashMultimap.create();
        
        for (int i = 0; i < pairs.length; i += 2) {
            String key = pairs[i];
            String value = pairs[i + 1];
            
            fields.put(key, new BaseNormalizedContent(key, value));
        }
        
        return fields;
    }
    
    /**
     * Creates a mock event with the given data type.
     *
     * @param dataType
     * @return mock event
     */
    public static RawRecordContainer createEvent(Type dataType) {
        RawRecordContainer event = EasyMock.createMock(RawRecordContainer.class);
        
        EasyMock.expect(event.getDataType()).andReturn(dataType).atLeastOnce();
        EasyMock.replay(event);
        
        return event;
    }
    
    /**
     * Helper to extract the String version of the row of a key/value pair.
     *
     * @param entry
     * @return row
     */
    public static String row(Map.Entry<BulkIngestKey,Value> entry) {
        return entry.getKey().getKey().getRow().toString();
    }
    
    /**
     * Helper to extract the String version of the column family of a key/value pair.
     *
     * @param entry
     * @return column family
     */
    public static String family(Map.Entry<BulkIngestKey,Value> entry) {
        return entry.getKey().getKey().getColumnFamily().toString();
    }
    
    /**
     * Helper to extract the String version of the column qualifier of a key/value pair.
     *
     * @param entry
     * @return column qualifier
     */
    public static String qualifier(Map.Entry<BulkIngestKey,Value> entry) {
        return entry.getKey().getKey().getColumnQualifier().toString();
    }
    
    /**
     * Helper to extract the String version of the value of a key/value pair.
     *
     * @param entry
     * @return value
     */
    public static String value(Map.Entry<BulkIngestKey,Value> entry) {
        return entry.getValue().toString();
    }
}
