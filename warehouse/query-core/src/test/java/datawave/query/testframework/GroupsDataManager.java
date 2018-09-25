package datawave.query.testframework;

import au.com.bytecode.opencsv.CSVReader;
import datawave.data.normalizer.Normalizer;
import datawave.query.testframework.GroupsDataType.GroupField;
import org.apache.log4j.Logger;
import org.junit.Assert;

import java.io.IOException;
import java.io.Reader;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class GroupsDataManager extends AbstractDataManager {
    
    private static final Logger log = Logger.getLogger(GroupsDataManager.class);
    
    public GroupsDataManager() {
        super(GroupField.EVENT_ID.name(), GroupField.START_DATE.name(), GroupField.getMetadata());
    }
    
    @Override
    public List<String> getHeaders() {
        return null;
    }
    
    @Override
    public void addTestData(URI file, String datatype, Set<String> indexes) throws IOException {
        Assert.assertFalse("datatype has already been configured(" + datatype + ")", this.rawData.containsKey(datatype));
        try (final Reader reader = Files.newBufferedReader(Paths.get(file)); final CSVReader csv = new CSVReader(reader)) {
            String[] data;
            int count = 0;
            Set<RawData> entries = new HashSet<>();
            while (null != (data = csv.readNext())) {
                final RawData raw = new GroupRawData(datatype, data, GroupField.getMetadata());
                entries.add(raw);
                count++;
            }
            this.rawData.put(datatype, entries);
            this.rawDataIndex.put(datatype, indexes);
            log.info("groups test data(" + file + ") count(" + count + ")");
        }
    }
    
    static class GroupRawData extends BaseRawData {
        
        private static final Map<String,RawMetaData> metadata = GroupField.getMetadata();
        
        GroupRawData(final String datatype, final String fields[], Map<String,RawMetaData> metaDataMap) {
            super(datatype, fields, GroupField.headers(), metadata);
            Assert.assertEquals("group ingest data field count is invalid", GroupField.headers().size(), fields.length);
        }
        
        @Override
        public String getKey(String field) {
            return GroupField.getQueryField(field);
        }
        
        @Override
        public boolean containsField(final String field) {
            return GroupField.headers().contains(field.toLowerCase());
        }
        
        @Override
        public boolean isMultiValueField(final String field) {
            final String query = GroupField.getQueryField(field);
            return metadata.get(query.toLowerCase()).multiValue;
        }
        
        @Override
        public boolean isTokenizedField(String field) {
            return GroupField.isTokenField(field);
        }
        
        @Override
        public Normalizer<?> getNormalizer(String field) {
            final String query = GroupField.getQueryField(field);
            return metadata.get(query.toLowerCase()).normalizer;
        }
    }
}
