package datawave.query.testframework;

import com.google.common.collect.Multimap;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.query.testframework.FlattenDataType.FlattenBaseFields;
import org.apache.log4j.Logger;
import org.junit.Assert;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Data manager for testing various flatteners for JSON data.
 */
public class FlattenDataManager extends AbstractDataManager {
    
    private static final Logger log = Logger.getLogger(FlattenDataManager.class);
    
    private final FlattenData flatData;
    
    /**
     *
     * @param data
     *            defines the configuration information for the requested flattener
     */
    public FlattenDataManager(final FlattenData data) {
        super(FlattenBaseFields.EVENTID.name(), FlattenBaseFields.STARTDATE.name(), data.getMetadata());
        this.flatData = data;
    }
    
    @Override
    public List<String> getHeaders() {
        return this.flatData.headers();
    }
    
    @Override
    public void addTestData(URI file, String datatype, Set<String> indexes) throws IOException {
        Assert.assertFalse("datatype has already been configured(" + datatype + ")", this.rawData.containsKey(datatype));
        FlattenDataType flatten = FlattenDataType.getFlattenDataType(datatype);
        DataLoader loader = new JsonTestFileLoader(file, flatten.getHadoopConfiguration());
        Collection<Multimap<String,NormalizedContentInterface>> rawData = loader.getRawData();
        
        Set<RawData> entries = new HashSet<>();
        for (Multimap<String,NormalizedContentInterface> entry : rawData) {
            Map<String,Collection<NormalizedContentInterface>> fields = entry.asMap();
            final RawData raw = new FlattenRawData(datatype, fields, this.flatData);
            entries.add(raw);
        }
        this.rawData.put(datatype, entries);
        this.rawDataIndex.put(datatype, indexes);
        
        log.debug("load test data complete (" + entries.size() + ")");
    }
    
    /**
     * Raw data entry for a flatten datatype.
     */
    static class FlattenRawData extends BaseRawData {
        
        private final FlattenData flattenData;
        
        /**
         * Constructor for populating data read from a JSON file.
         * 
         * @param datatype
         *            name of datatype
         * @param fields
         *            mapping of fields to values
         * @param data
         *            flatten configuration info
         */
        FlattenRawData(final String datatype, Map<String,Collection<NormalizedContentInterface>> fields, final FlattenData data) {
            super(datatype, data.headers(), data.getMetadata());
            Assert.assertEquals("flatten ingest data field count is invalid", data.headers().size(), fields.size());
            this.flattenData = data;
            processNormalizedContent(datatype, fields);
        }
        
        @Override
        public boolean containsField(final String field) {
            List<String> fields = this.flattenData.headers();
            return fields.contains(field.toUpperCase());
        }
    }
}
