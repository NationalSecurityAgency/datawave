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
public class CityPOJOManager extends AbstractPOJOManager {
    
    private static final Logger log = Logger.getLogger(CityPOJOManager.class);
    
    public CityPOJOManager() {
        super(CityField.EVENT_ID.name());
    }
    
    @Override
    public void addTestData(final URI file) throws IOException {
        try (final Reader reader = Files.newBufferedReader(Paths.get(file)); final CSVReader csv = new CSVReader(reader)) {
            String[] data;
            int count = 0;
            while (null != (data = csv.readNext())) {
                final IRawData pojo = new CityRawData(data);
                this.pojos.add(pojo);
                count++;
            }
            log.info("city test data(" + file + ") count(" + count + ")");
        }
    }
    
    @Override
    public Set<IRawData> findMatchers(QueryAction action, final Date startDate, final Date endDate) {
        Set<String> fields = new HashSet<>();
        if (Constants.ANY_FIELD.equals(action.getKey())) {
            fields.addAll(CityField.anyFieldIndex());
        } else {
            fields.add(action.getKey());
        }
        
        final Map<String,Type> types = CityField.getFieldTypeMapping();
        Collection<IRawData> data = this.getRawData(startDate, endDate);
        return matchField(fields, types, action, data);
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
    
    // @Override
    // public Set<IRawData> getRawData() {
    // return this.pojos;
    // }
    
    @Override
    public Date[] getRandomStartEndDate() {
        return CitiesDataType.CityShardId.generateRandomStartEndDates();
    }
    
    private Set<IRawData> getRawData(final Date start, final Date end) {
        final Set<IRawData> data = new HashSet<>(this.pojos.size());
        final Set<String> shards = CitiesDataType.CityShardId.getShardRange(start, end);
        for (final IRawData pojo : this.pojos) {
            String id = pojo.getValue(CityField.START_DATE.name());
            if (shards.contains(id)) {
                data.add(pojo);
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
