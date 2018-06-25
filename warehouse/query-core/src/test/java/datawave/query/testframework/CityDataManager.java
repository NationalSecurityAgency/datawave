package datawave.query.testframework;

import au.com.bytecode.opencsv.CSVReader;
import datawave.query.Constants;
import datawave.query.testframework.CitiesDataType.CityField;
import org.apache.log4j.Logger;
import org.junit.Assert;

import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.Type;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Provides a mapping of the raw ingest data for query analysis. This will allow dynamic calculation of expected results and modification of the test data
 * without impacting the test cases. Each entry in the file will be transformed to a POJO entry.
 * <p>
 * </p>
 * This class is immutable.
 */
public class CityDataManager extends AbstractDataManager {
    
    private static final Logger log = Logger.getLogger(CityDataManager.class);
    
    public CityDataManager() {
        super(CityField.EVENT_ID.name());
    }
    
    @Override
    public void addTestData(final URI file, final String datatype, final Set<String> indexes) throws IOException {
        Assert.assertFalse("datatype has already been configured(" + datatype + ")", this.rawData.containsKey(datatype));
        try (final Reader reader = Files.newBufferedReader(Paths.get(file)); final CSVReader csv = new CSVReader(reader)) {
            String[] data;
            int count = 0;
            Set<IRawData> cityData = new HashSet<>();
            while (null != (data = csv.readNext())) {
                final IRawData raw = new CityRawData(data);
                cityData.add(raw);
                count++;
            }
            this.rawData.put(datatype, cityData);
            this.rawDataIndex.put(datatype, indexes);
            log.info("city test data(" + file + ") count(" + count + ")");
        }
    }
    
    @Override
    public Set<IRawData> findMatchers(QueryAction action, final Date startDate, final Date endDate) {
        final Map<String,Type> types = CityField.getFieldTypeMapping();
        
        final Set<IRawData> matches = new HashSet<>();
        for (final String dataType : this.rawData.keySet()) {
            Collection<IRawData> data = this.getRawData(this.rawData.get(dataType), startDate, endDate);
            
            Set<String> fields = new HashSet<>();
            if (Constants.ANY_FIELD.equals(action.getKey())) {
                fields.addAll(this.rawDataIndex.get(dataType));
            } else {
                fields.add(action.getKey());
            }
            
            matches.addAll(matchField(fields, types, action, data));
        }
        
        return matches;
    }
    
    @Override
    public Type getFieldType(String field) {
        return CityField.getFieldType(field);
    }
    
    @Override
    public Set<String> getKeyField(Set<IRawData> entries) {
        final Set<String> keys = new HashSet<>(entries.size());
        for (IRawData entry : entries) {
            keys.add(entry.getValue(this.rawKeyField));
        }
        
        return keys;
    }
    
    @Override
    public List<String> getHeaders() {
        return CityField.headers();
    }
    
    @Override
    public Date[] getRandomStartEndDate() {
        return CitiesDataType.CityShardId.getStartEndDates(true);
    }
    
    @Override
    public Date[] getShardStartEndDate() {
        return CitiesDataType.CityShardId.getStartEndDates(false);
    }
    
    private Set<IRawData> getRawData(final Set<IRawData> rawData, final Date start, final Date end) {
        final Set<IRawData> data = new HashSet<>(this.rawData.size());
        final Set<String> shards = CitiesDataType.CityShardId.getShardRange(start, end);
        for (final IRawData raw : rawData) {
            String id = raw.getValue(CityField.START_DATE.name());
            if (shards.contains(id)) {
                data.add(raw);
            }
        }
        
        return data;
    }
    
    static class CityRawData extends BaseRawData {
        
        private static final Map<String,RawMetaData> metadata = new HashMap<>();
        static {
            for (final CityField field : CityField.values()) {
                metadata.put(field.name(), field.getMetadata());
            }
        }
        
        CityRawData(final String fields[]) {
            super(fields);
            Assert.assertEquals("city ingest data field count is invalid", CityField.headers().size(), fields.length);
        }
        
        @Override
        protected List<String> getHeaders() {
            return CityField.headers();
        }
        
        @Override
        protected boolean containsField(final String field) {
            return CityField.headers().contains(field);
        }
        
        @Override
        public boolean isMultiValueField(final String field) {
            return metadata.get(field).multiValue;
        }
        
        @Override
        public Type getFieldType(final String field) {
            return metadata.containsKey(field) ? metadata.get(field).type : null;
        }
    }
}
