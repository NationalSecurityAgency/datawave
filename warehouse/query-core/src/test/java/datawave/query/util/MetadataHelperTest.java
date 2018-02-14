package datawave.query.util;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.IdentityDataType;
import datawave.TestBaseIngestHelper;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.data.type.Type;
import datawave.ingest.config.RawRecordContainerImpl;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;
import datawave.ingest.data.config.ingest.BaseIngestHelper;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.metadata.EventMetadata;
import datawave.ingest.metadata.RawRecordMetadata;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MetadataHelperTest {
    
    private static final String METADATA_TABLE_NAME = "DatawaveMetadata";
    private static final String LOADDATES_TABLE_NAME = "loaddates";
    private static final String INDEX_TABLE_NAME = "index";
    private static final String RINDEX_TABLE_NAME = "reverseIndex";
    
    private static final String FIELD_NAME = "SOME_FIELD";
    private static final String[] MAPPED_FIELDS = new String[] {"THIS_FIELD", "THAT_FIELD", "ANOTHER_FIELD"};
    private static final String DATA_TYPE = "SOME_DATA_TYPE";
    private static final String BEGIN_DATE = "20000101 000000.000";
    
    private static final String PASSWORD = "";
    private static final Authorizations AUTHS = new Authorizations(new String[] {"THESE", "ARE", "ALL", "THE", "AUTHS", "CAN", "YOU", "DIG", "IT"});
    
    private static final String formatPattern = "yyyyMMdd HHmmss.SSS";
    private static final SimpleDateFormat formatter = new SimpleDateFormat(formatPattern);
    
    private static BaseIngestHelper helper;
    
    private static InMemoryInstance instance;
    
    @BeforeClass
    public static void setup() throws Exception {
        // write these values to their respective tables
        instance = new InMemoryInstance();
        Connector connector = instance.getConnector("root", PASSWORD);
        connector.securityOperations().changeUserAuthorizations("root", AUTHS);
        
        helper = new TestBaseIngestHelper(createEventFields()) {
            @Override
            public List<Type<?>> getDataTypes(String fieldName) {
                return Arrays.<datawave.data.type.Type<?>> asList(new IdentityDataType());
            }
        };
        helper.addIndexedField(FIELD_NAME);
        helper.addReverseIndexedField(FIELD_NAME);
        helper.addFieldIndexFilterFieldMapping(FIELD_NAME, Arrays.asList(MAPPED_FIELDS));
        
        RawRecordContainer record = new RawRecordContainerImpl();
        record.clear();
        record.setDataType(new datawave.ingest.data.Type(DATA_TYPE, helper.getClass(), (Class) null, (String[]) null, 1, (String[]) null));
        record.setRawFileName("someData.dat");
        record.setRawRecordNumber(0);
        record.setDate(formatter.parse(BEGIN_DATE).getTime());
        record.setRawDataAndGenerateId("some raw data".getBytes("UTF8"));
        record.setVisibility(new ColumnVisibility("ALL"));
        
        RawRecordMetadata eventMetadata = new EventMetadata(null, new Text(METADATA_TABLE_NAME), new Text(LOADDATES_TABLE_NAME), new Text(INDEX_TABLE_NAME),
                        new Text(RINDEX_TABLE_NAME), true);
        eventMetadata.addEvent(helper, record, createEventFields());
        
        Multimap<BulkIngestKey,Value> bulkMetadata = eventMetadata.getBulkMetadata();
        writeKeyValues(connector, bulkMetadata);
    }
    
    private static Multimap<String,NormalizedContentInterface> createEventFields() {
        Multimap<String,NormalizedContentInterface> eventFields = HashMultimap.create();
        for (String mappedField : MAPPED_FIELDS)
            addToEventFields(eventFields, mappedField, mappedField + " DOESN'T MATTER");
        addToEventFields(eventFields, FIELD_NAME, "HEY HO HEY HO");
        return eventFields;
    }
    
    private static void addToEventFields(Multimap<String,NormalizedContentInterface> eventFields, String fieldName, String value) {
        NormalizedContentInterface content = new NormalizedFieldAndValue(fieldName, value);
        eventFields.put(fieldName, content);
    }
    
    private static void writeKeyValues(Connector connector, Multimap<BulkIngestKey,Value> keyValues) throws Exception {
        final TableOperations tops = connector.tableOperations();
        final Set<BulkIngestKey> biKeys = keyValues.keySet();
        for (final BulkIngestKey biKey : biKeys) {
            final String tableName = biKey.getTableName().toString();
            if (!tops.exists(tableName))
                tops.create(tableName);
            
            final BatchWriter writer = connector.createBatchWriter(tableName, new BatchWriterConfig());
            for (final Value val : keyValues.get(biKey)) {
                final Mutation mutation = new Mutation(biKey.getKey().getRow());
                mutation.put(biKey.getKey().getColumnFamily(), biKey.getKey().getColumnQualifier(), biKey.getKey().getColumnVisibilityParsed(), val);
                writer.addMutation(mutation);
            }
            writer.close();
        }
    }
    
    @Test
    public void testFieldIndexFilterMapByType() throws Exception {
        MetadataHelper metadataHelper = MetadataHelper.getInstance(instance.getConnector("root", PASSWORD), METADATA_TABLE_NAME, Collections.singleton(AUTHS));
        
        Map<String,Multimap<String,String>> fieldIndexFilterMapByType = metadataHelper.getFieldIndexFilterMapByType();
        Assert.assertEquals(1, fieldIndexFilterMapByType.size());
        
        Multimap<String,String> fieldIndexFilterMap = fieldIndexFilterMapByType.get(DATA_TYPE);
        Assert.assertNotNull(fieldIndexFilterMap);
        Assert.assertEquals(1, fieldIndexFilterMap.keySet().size());
        
        Collection<String> mappedFields = fieldIndexFilterMap.get(FIELD_NAME);
        Assert.assertNotNull(mappedFields);
        
        Set<String> expectedMappedFields = new HashSet<>(Arrays.asList(MAPPED_FIELDS));
        Assert.assertEquals(expectedMappedFields, mappedFields);
    }
    
    @Test
    public void testFieldIndexFilterMap() throws Exception {
        MetadataHelper metadataHelper = MetadataHelper.getInstance(instance.getConnector("root", PASSWORD), METADATA_TABLE_NAME, Collections.singleton(AUTHS));
        
        Multimap<String,String> fieldIndexFilterMap = metadataHelper.getFieldIndexFilterMap(DATA_TYPE);
        Assert.assertNotNull(fieldIndexFilterMap);
        Assert.assertEquals(1, fieldIndexFilterMap.keySet().size());
        
        Collection<String> mappedFields = fieldIndexFilterMap.get(FIELD_NAME);
        Assert.assertNotNull(mappedFields);
        
        Set<String> expectedMappedFields = new HashSet<>(Arrays.asList(MAPPED_FIELDS));
        Assert.assertEquals(expectedMappedFields, mappedFields);
    }
    
    @Test
    public void testFieldIndexFilterFields() throws Exception {
        MetadataHelper metadataHelper = MetadataHelper.getInstance(instance.getConnector("root", PASSWORD), METADATA_TABLE_NAME, Collections.singleton(AUTHS));
        
        Collection<String> mappedFields = metadataHelper.getFieldIndexFilterFields(DATA_TYPE, FIELD_NAME);
        Assert.assertNotNull(mappedFields);
        
        Set<String> expectedMappedFields = new HashSet<>(Arrays.asList(MAPPED_FIELDS));
        Assert.assertEquals(expectedMappedFields, mappedFields);
    }
    
    @Test(expected = UnsupportedOperationException.class)
    public void testFieldIndexFilterMapByTypeUnmodifiable() throws Exception {
        MetadataHelper metadataHelper = MetadataHelper.getInstance(instance.getConnector("root", PASSWORD), METADATA_TABLE_NAME, Collections.singleton(AUTHS));
        
        Map<String,Multimap<String,String>> fieldIndexFilterMapByType = metadataHelper.getFieldIndexFilterMapByType();
        fieldIndexFilterMapByType.remove(DATA_TYPE);
    }
    
    @Test(expected = UnsupportedOperationException.class)
    public void testFieldIndexFilterMapUnmodifiable() throws Exception {
        MetadataHelper metadataHelper = MetadataHelper.getInstance(instance.getConnector("root", PASSWORD), METADATA_TABLE_NAME, Collections.singleton(AUTHS));
        
        Map<String,Multimap<String,String>> fieldIndexFilterMapByType = metadataHelper.getFieldIndexFilterMapByType();
        fieldIndexFilterMapByType.get(DATA_TYPE).removeAll(FIELD_NAME);
    }
    
    @Test(expected = UnsupportedOperationException.class)
    public void testFieldIndexFilterFieldsUnmodifiable() throws Exception {
        MetadataHelper metadataHelper = MetadataHelper.getInstance(instance.getConnector("root", PASSWORD), METADATA_TABLE_NAME, Collections.singleton(AUTHS));
        
        Map<String,Multimap<String,String>> fieldIndexFilterMapByType = metadataHelper.getFieldIndexFilterMapByType();
        fieldIndexFilterMapByType.get(DATA_TYPE).get(FIELD_NAME).remove(MAPPED_FIELDS[0]);
    }
}
