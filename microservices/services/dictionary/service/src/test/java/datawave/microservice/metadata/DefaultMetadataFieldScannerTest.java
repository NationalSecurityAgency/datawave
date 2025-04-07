package datawave.microservice.metadata;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.data.ColumnFamilyConstants;
import datawave.marking.MarkingFunctions;
import datawave.microservice.Connection;
import datawave.microservice.dictionary.config.ResponseObjectFactory;
import datawave.webservice.dictionary.data.DefaultDataDictionary;
import datawave.webservice.dictionary.data.DefaultDescription;
import datawave.webservice.dictionary.data.DefaultDictionaryField;
import datawave.webservice.dictionary.data.DefaultFields;
import datawave.webservice.metadata.DefaultMetadataField;

public class DefaultMetadataFieldScannerTest {
    
    private static final String DATE = "20200115051230";
    private static final long TIMESTAMP = ZonedDateTime.of(LocalDateTime.of(2020, 1, 15, 5, 12, 30), ZoneId.systemDefault()).toInstant().toEpochMilli();
    private static final long DAY_AS_MILLISECONDS = 86400000;
    private static final String MODEL_TABLE = "modelTable";
    private static final String METADATA_TABLE = "metadataTable";
    private static final String[] AUTH = {"PRIVATE"};
    private static final Set<Authorizations> AUTHS = Collections.singleton(new Authorizations(AUTH));
    
    private static final ResponseObjectFactory<DefaultDescription,DefaultDataDictionary,DefaultMetadataField,DefaultDictionaryField,DefaultFields> RESPONSE_OBJECT_FACTORY = new ResponseObjectFactory<DefaultDescription,DefaultDataDictionary,DefaultMetadataField,DefaultDictionaryField,DefaultFields>() {
        @Override
        public DefaultDataDictionary getDataDictionary() {
            return null;
        }
        
        @Override
        public DefaultDescription getDescription() {
            return new DefaultDescription();
        }
        
        @Override
        public DefaultFields getFields() {
            return new DefaultFields();
        }
    };
    
    private AccumuloClient connector;
    
    private Map<String,String> expectedTimestamps;
    
    private DefaultMetadataFieldScanner scanner;
    
    @BeforeEach
    public void setUp() throws Exception {
        InMemoryInstance instance = new InMemoryInstance();
        connector = new InMemoryAccumuloClient("root", instance);
        connector.securityOperations().changeUserAuthorizations("root", new Authorizations(AUTH));
        connector.tableOperations().create(METADATA_TABLE);
        connector.tableOperations().create(MODEL_TABLE);
        
        expectedTimestamps = new HashMap<>();
        
        populateMetadataTable();
        
        Map<String,String> normalizerMapping = new HashMap<>();
        normalizerMapping.put("datawave.data.type.LcNoDiacriticsType", "Text");
        normalizerMapping.put("datawave.data.type.NumberType", "Number");
        
        Connection connectionConfig = new Connection();
        connectionConfig.setAccumuloClient(connector);
        connectionConfig.setMetadataTable(METADATA_TABLE);
        connectionConfig.setAuths(AUTHS);
        
        scanner = new DefaultMetadataFieldScanner(new MarkingFunctions.Default(), RESPONSE_OBJECT_FACTORY, normalizerMapping, connectionConfig, 1);
    }
    
    @Test
    public void whenRetrievingFields_givenNoDataTypeFilters_shouldReturnUnfilteredResults() throws TableNotFoundException {
        DefaultMetadataField barField = new DefaultMetadataField();
        barField.setFieldName("BAR_FIELD");
        barField.setDataType("csv");
        barField.setForwardIndexed(true);
        barField.setReverseIndexed(true);
        barField.setTokenized(true);
        barField.setTypes(Collections.singletonList("Text"));
        barField.setDescription(Collections.singleton(createDescription("Barfield Description")));
        barField.setLastUpdated(DATE);
        
        DefaultMetadataField contributorId = new DefaultMetadataField();
        contributorId.setFieldName("CONTRIBUTOR_ID");
        contributorId.setDataType("enwiki");
        contributorId.setForwardIndexed(true);
        contributorId.setTypes(Collections.singletonList("Number"));
        contributorId.setDescription(Collections.singleton(createDescription("ContributorId Description")));
        contributorId.setLastUpdated(DATE);
        
        DefaultMetadataField name = new DefaultMetadataField();
        name.setFieldName("NAME");
        name.setDataType("tvmaze");
        name.setForwardIndexed(true);
        name.setReverseIndexed(true);
        name.setTypes(Collections.singletonList("Unknown"));
        name.setLastUpdated(DATE);
        
        DefaultMetadataField fooToken = new DefaultMetadataField();
        fooToken.setFieldName("FOO_TOKEN");
        fooToken.setDataType("tvmaze");
        fooToken.setForwardIndexed(true);
        fooToken.setReverseIndexed(false);
        fooToken.setTokenized(true);
        fooToken.setIndexOnly(true);
        fooToken.setLastUpdated("20200120051230");
        fooToken.setTypes(Collections.singletonList("Text"));
        
        Collection<DefaultMetadataField> fields = scanner.getFields(Collections.emptyMap(), Collections.emptySet());
        assertThat(fields).containsExactlyInAnyOrder(barField, contributorId, name, fooToken);
    }
    
    @Test
    public void whenRetrievingFields_givenDataTypeFilters_shouldReturnFilteredResults() throws TableNotFoundException {
        DefaultMetadataField barField = new DefaultMetadataField();
        barField.setFieldName("BAR_FIELD");
        barField.setDataType("csv");
        barField.setForwardIndexed(true);
        barField.setReverseIndexed(true);
        barField.setTokenized(true);
        barField.setTypes(Collections.singletonList("Text"));
        barField.setDescription(Collections.singleton(createDescription("Barfield Description")));
        barField.setLastUpdated(DATE);
        
        DefaultMetadataField contributorId = new DefaultMetadataField();
        contributorId.setFieldName("CONTRIBUTOR_ID");
        contributorId.setDataType("enwiki");
        contributorId.setForwardIndexed(true);
        contributorId.setTypes(Collections.singletonList("Number"));
        contributorId.setDescription(Collections.singleton(createDescription("ContributorId Description")));
        contributorId.setLastUpdated(DATE);
        
        Set<String> dataTypeFilters = new HashSet<>();
        dataTypeFilters.add("csv");
        dataTypeFilters.add("enwiki");
        Collection<DefaultMetadataField> fields = scanner.getFields(Collections.emptyMap(), dataTypeFilters);
        assertThat(fields).containsExactlyInAnyOrder(barField, contributorId);
    }
    
    @Test
    public void whenRetrievingFields_givenAliases_shouldReturnResultsWithAliases() throws TableNotFoundException {
        DefaultMetadataField barField = new DefaultMetadataField();
        barField.setFieldName("bar_field_alias");
        barField.setInternalFieldName("BAR_FIELD");
        barField.setDataType("csv");
        barField.setForwardIndexed(true);
        barField.setReverseIndexed(true);
        barField.setTokenized(true);
        barField.setTypes(Collections.singletonList("Text"));
        barField.setDescription(Collections.singleton(createDescription("Barfield Description")));
        barField.setLastUpdated(DATE);
        
        DefaultMetadataField contributorId = new DefaultMetadataField();
        contributorId.setFieldName("contributor_id_alias");
        contributorId.setInternalFieldName("CONTRIBUTOR_ID");
        contributorId.setDataType("enwiki");
        contributorId.setForwardIndexed(true);
        contributorId.setTypes(Collections.singletonList("Number"));
        contributorId.setDescription(Collections.singleton(createDescription("ContributorId Description")));
        contributorId.setLastUpdated(DATE);
        
        DefaultMetadataField name = new DefaultMetadataField();
        name.setFieldName("NAME");
        name.setDataType("tvmaze");
        name.setForwardIndexed(true);
        name.setReverseIndexed(true);
        name.setTypes(Collections.singletonList("Unknown"));
        name.setLastUpdated(DATE);
        
        DefaultMetadataField fooToken = new DefaultMetadataField();
        fooToken.setFieldName("FOO_TOKEN");
        fooToken.setDataType("tvmaze");
        fooToken.setForwardIndexed(true);
        fooToken.setReverseIndexed(false);
        fooToken.setTokenized(true);
        fooToken.setIndexOnly(true);
        fooToken.setLastUpdated("20200120051230");
        fooToken.setTypes(Collections.singletonList("Text"));
        
        Map<String,String> aliases = new HashMap<>();
        aliases.put("BAR_FIELD", "bar_field_alias");
        aliases.put("CONTRIBUTOR_ID", "contributor_id_alias");
        Collection<DefaultMetadataField> fields = scanner.getFields(aliases, Collections.emptySet());
        assertThat(fields).containsExactlyInAnyOrder(barField, contributorId, name, fooToken);
    }
    
    @Test
    public void lastUpdatedTimeIsCorrect() throws Exception {
        Collection<DefaultMetadataField> fields = scanner.getFields(Collections.emptyMap(), Collections.emptySet());
        
        for (DefaultMetadataField field : fields) {
            assertThat(expectedTimestamps).containsEntry(field.getFieldName(), field.getLastUpdated());
        }
    }
    
    private String formatTimestamp(Long timestamp) {
        return Instant.ofEpochMilli(timestamp).atZone(ZoneId.systemDefault()).toLocalDateTime().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
    }
    
    private void populateMetadataTable() throws TableNotFoundException, MutationsRejectedException {
        Mutation barField = new Mutation(new Text("BAR_FIELD"));
        barField.put(new Text(ColumnFamilyConstants.COLF_E), new Text("csv"), TIMESTAMP, new Value());
        barField.put(new Text(ColumnFamilyConstants.COLF_I), new Text("csv"), TIMESTAMP, new Value());
        barField.put(new Text(ColumnFamilyConstants.COLF_RI), new Text("csv"), TIMESTAMP, new Value());
        barField.put(new Text(ColumnFamilyConstants.COLF_TF), new Text("csv"), TIMESTAMP, new Value());
        barField.put(new Text(ColumnFamilyConstants.COLF_T), new Text("csv\0datawave.data.type.LcNoDiacriticsType"), TIMESTAMP, new Value());
        barField.put(new Text(ColumnFamilyConstants.COLF_DESC), new Text("csv"), new ColumnVisibility("PRIVATE"), TIMESTAMP, new Value("Barfield Description"));
        expectedTimestamps.put("BAR_FIELD", formatTimestamp(TIMESTAMP));
        
        Mutation contributorId = new Mutation(new Text("CONTRIBUTOR_ID"));
        contributorId.put(new Text(ColumnFamilyConstants.COLF_E), new Text("enwiki"), TIMESTAMP, new Value());
        contributorId.put(new Text(ColumnFamilyConstants.COLF_I), new Text("enwiki"), TIMESTAMP, new Value());
        contributorId.put(new Text(ColumnFamilyConstants.COLF_T), new Text("enwiki\0datawave.data.type.NumberType"), TIMESTAMP, new Value());
        contributorId.put(new Text(ColumnFamilyConstants.COLF_DESC), new Text("enwiki"), new ColumnVisibility("PRIVATE"), TIMESTAMP,
                        new Value("ContributorId Description"));
        expectedTimestamps.put("CONTRIBUTOR_ID", formatTimestamp(TIMESTAMP));
        
        Mutation name = new Mutation(new Text("NAME"));
        name.put(new Text(ColumnFamilyConstants.COLF_E), new Text("tvmaze"), TIMESTAMP, new Value());
        name.put(new Text(ColumnFamilyConstants.COLF_I), new Text("tvmaze"), TIMESTAMP, new Value());
        name.put(new Text(ColumnFamilyConstants.COLF_RI), new Text("tvmaze"), TIMESTAMP, new Value());
        name.put(new Text(ColumnFamilyConstants.COLF_T), new Text("tvmaze\0not.a.known.type"), TIMESTAMP, new Value());
        expectedTimestamps.put("NAME", formatTimestamp(TIMESTAMP));
        
        Mutation fooToken = new Mutation(new Text("FOO_TOKEN"));
        fooToken.put(new Text(ColumnFamilyConstants.COLF_I), new Text("tvmaze"), TIMESTAMP, new Value());
        fooToken.put(new Text(ColumnFamilyConstants.COLF_I), new Text("tvmaze"), TIMESTAMP + (DAY_AS_MILLISECONDS * 2), new Value());
        fooToken.put(new Text(ColumnFamilyConstants.COLF_I), new Text("tvmaze"), TIMESTAMP + (DAY_AS_MILLISECONDS * 3), new Value());
        fooToken.put(new Text(ColumnFamilyConstants.COLF_I), new Text("tvmaze"), TIMESTAMP - (DAY_AS_MILLISECONDS * 2), new Value());
        fooToken.put(new Text(ColumnFamilyConstants.COLF_I), new Text("tvmaze"), TIMESTAMP - (DAY_AS_MILLISECONDS * 3), new Value());
        fooToken.put(new Text(ColumnFamilyConstants.COLF_I), new Text("tvmaze"), TIMESTAMP + (DAY_AS_MILLISECONDS * 4), new Value());
        fooToken.put(new Text(ColumnFamilyConstants.COLF_I), new Text("tvmaze"), TIMESTAMP + (DAY_AS_MILLISECONDS * 5), new Value());
        fooToken.put(new Text(ColumnFamilyConstants.COLF_TF), new Text("tvmaze"), TIMESTAMP, new Value());
        fooToken.put(new Text(ColumnFamilyConstants.COLF_T), new Text("tvmaze\0datawave.data.type.LcNoDiacriticsType"), TIMESTAMP, new Value());
        expectedTimestamps.put("FOO_TOKEN", formatTimestamp(TIMESTAMP + (DAY_AS_MILLISECONDS * 5)));
        
        BatchWriterConfig bwConfig = new BatchWriterConfig().setMaxMemory(10L).setMaxLatency(1, TimeUnit.SECONDS).setMaxWriteThreads(1);
        BatchWriter writer = connector.createBatchWriter(METADATA_TABLE, bwConfig);
        writer.addMutation(barField);
        writer.addMutation(contributorId);
        writer.addMutation(name);
        writer.addMutation(fooToken);
        writer.flush();
        writer.close();
    }
    
    private DefaultDescription createDescription(String descriptionText) {
        DefaultDescription description = new DefaultDescription();
        description.setDescription(descriptionText);
        
        Map<String,String> markings = new HashMap<>();
        markings.put("columnVisibility", "PRIVATE}");
        description.setMarkings(markings);
        
        return description;
    }
}
