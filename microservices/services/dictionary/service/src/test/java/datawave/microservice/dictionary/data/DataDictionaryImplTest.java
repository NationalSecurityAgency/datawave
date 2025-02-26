package datawave.microservice.dictionary.data;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;

import datawave.marking.MarkingFunctions;
import datawave.microservice.Connection;
import datawave.microservice.dictionary.config.ResponseObjectFactory;
import datawave.microservice.metadata.MetadataDescriptionsHelper;
import datawave.microservice.metadata.MetadataDescriptionsHelperFactory;
import datawave.query.model.QueryModel;
import datawave.query.util.MetadataEntry;
import datawave.query.util.MetadataHelper;
import datawave.query.util.MetadataHelperFactory;
import datawave.webservice.dictionary.data.DefaultDataDictionary;
import datawave.webservice.dictionary.data.DefaultDescription;
import datawave.webservice.dictionary.data.DefaultDictionaryField;
import datawave.webservice.dictionary.data.DefaultFields;
import datawave.webservice.metadata.DefaultMetadataField;

@ExtendWith(MockitoExtension.class)
public class DataDictionaryImplTest {
    
    private static final String METADATA_TABLE = "metadataTable";
    private static final String MODEL_TABLE = "modelTable";
    private static final String FIELD_NAME = "field";
    private static final String MODEL_NAME = "model";
    private static final String DATATYPE = "csv";
    private static final Set<Authorizations> AUTHS = Collections.singleton(new Authorizations("PRIVATE"));
    
    private Connection connectionConfig;
    
    @Mock
    private AccumuloClient accumuloClient;
    
    @Mock
    private MarkingFunctions markingFunctions;
    
    @Mock
    private ResponseObjectFactory<DefaultDescription,DefaultDataDictionary,DefaultMetadataField,DefaultDictionaryField,DefaultFields> responseObjectFactory;
    
    @Mock
    private MetadataHelperFactory metadataHelperFactory;
    
    @Mock
    private MetadataDescriptionsHelperFactory<DefaultDescription> metadataDescriptionsHelperFactory;
    
    @Mock
    private MetadataDescriptionsHelper<DefaultDescription> metadataDescriptionsHelper;
    
    private DataDictionaryImpl dataDictionary;
    
    @BeforeEach
    public void setUp() {
        connectionConfig = new Connection();
        connectionConfig.setAccumuloClient(accumuloClient);
        connectionConfig.setAuths(AUTHS);
        connectionConfig.setMetadataTable(METADATA_TABLE);
        connectionConfig.setModelTable(MODEL_TABLE);
        connectionConfig.setModelName(MODEL_NAME);
        dataDictionary = new DataDictionaryImpl(markingFunctions, responseObjectFactory, metadataHelperFactory, metadataDescriptionsHelperFactory);
    }
    
    @Test
    public void whenSettingDescription_givenSingleDefaultDescription_shouldSetDescription() throws Exception {
        Map<String,String> markings = new HashMap<>();
        markings.put("columnVisibility", "PRIVATE");
        
        DefaultDescription description = new DefaultDescription();
        description.setMarkings(markings);
        description.setDescription("my ultra cool description");
        
        // Ensure no alias will be found.
        givenQueryModelReverseMapping(FIELD_NAME, "noalias");
        givenInitializedMetadataDescriptionsHelper();
        
        // Execute function under test.
        dataDictionary.setDescription(connectionConfig, FIELD_NAME, DATATYPE, description);
        
        // Verify expected calls.
        Set<DefaultDescription> descriptions = Collections.singleton(description);
        verify(metadataDescriptionsHelper).initialize(accumuloClient, METADATA_TABLE, AUTHS);
        verify(metadataDescriptionsHelper).setDescriptions(new MetadataEntry(FIELD_NAME, DATATYPE), descriptions);
    }
    
    @Test
    public void whenSettingDescription_givenDefaultDictionaryField_shouldSetDescriptionFromProperties() throws Exception {
        Map<String,String> markings = new HashMap<>();
        markings.put("columnVisibility", "PRIVATE");
        
        DefaultDescription description = new DefaultDescription();
        description.setMarkings(markings);
        description.setDescription("my ultra cool description");
        
        Set<DefaultDescription> descriptions = Collections.singleton(description);
        
        DefaultDictionaryField dictionaryField = new DefaultDictionaryField();
        dictionaryField.setFieldName(FIELD_NAME);
        dictionaryField.setDatatype(DATATYPE);
        dictionaryField.setDescriptions(descriptions);
        
        // Ensure no alias will be found.
        givenQueryModelReverseMapping(FIELD_NAME, "noalias");
        givenInitializedMetadataDescriptionsHelper();
        
        // Execute function under test.
        dataDictionary.setDescription(connectionConfig, dictionaryField);
        
        // Verify expected calls.
        verify(metadataDescriptionsHelper).initialize(accumuloClient, METADATA_TABLE, AUTHS);
        verify(metadataDescriptionsHelper).setDescriptions(new MetadataEntry(FIELD_NAME, DATATYPE), descriptions);
    }
    
    @Test
    public void whenSettingDescription_givenNoAlias_shouldSetDescriptionWithOriginalFieldName() throws Exception {
        Map<String,String> markings = new HashMap<>();
        markings.put("columnVisibility", "PRIVATE");
        
        DefaultDescription description = new DefaultDescription();
        description.setMarkings(markings);
        description.setDescription("my ultra cool description");
        
        Set<DefaultDescription> descriptions = Collections.singleton(description);
        
        // Ensure no alias will be found.
        givenQueryModelReverseMapping(FIELD_NAME, "noalias");
        givenInitializedMetadataDescriptionsHelper();
        
        // Execute function under test.
        dataDictionary.setDescriptions(connectionConfig, FIELD_NAME, DATATYPE, descriptions);
        
        // Verify expected calls.
        verify(metadataDescriptionsHelper).initialize(accumuloClient, METADATA_TABLE, AUTHS);
        verify(metadataDescriptionsHelper).setDescriptions(new MetadataEntry(FIELD_NAME, DATATYPE), descriptions);
    }
    
    @Test
    public void whenSettingDescription_givenAlias_shouldSetDescriptionWithAlias() throws Exception {
        Map<String,String> markings = new HashMap<>();
        markings.put("columnVisibility", "PRIVATE");
        
        DefaultDescription description = new DefaultDescription();
        description.setMarkings(markings);
        description.setDescription("my ultra cool description");
        
        Set<DefaultDescription> descriptions = Collections.singleton(description);
        
        // Ensure an alias will be found.
        givenQueryModelReverseMapping("alias", FIELD_NAME);
        givenInitializedMetadataDescriptionsHelper();
        
        // Execute function under test.
        dataDictionary.setDescriptions(connectionConfig, FIELD_NAME, DATATYPE, descriptions);
        
        // Verify expected calls.
        verify(metadataDescriptionsHelper).initialize(accumuloClient, METADATA_TABLE, AUTHS);
        verify(metadataDescriptionsHelper).setDescriptions(new MetadataEntry("alias", DATATYPE), descriptions);
    }
    
    @Test
    public void whenRetrievingDescriptions_shouldRetrieveDescriptions() throws Exception {
        // Set the given field aliases.
        Map<String,String> queryModelMapping = new HashMap<>();
        queryModelMapping.put("FIELDA", "aliasA");
        queryModelMapping.put("FIELDB", "aliasB");
        givenQueryModelReverseMapping(queryModelMapping);
        
        // Set the descriptions to be returned by the helper.
        DefaultDescription descriptionA = new DefaultDescription("fieldA description");
        DefaultDescription descriptionB = new DefaultDescription("fieldB description");
        DefaultDescription descriptionC = new DefaultDescription("fieldC description");
        
        SetMultimap<MetadataEntry,DefaultDescription> descriptions = HashMultimap.create();
        descriptions.put(new MetadataEntry("fieldA", DATATYPE), descriptionA);
        descriptions.put(new MetadataEntry("fieldB", DATATYPE), descriptionB);
        descriptions.put(new MetadataEntry("fieldC", DATATYPE), descriptionC);
        
        givenInitializedMetadataDescriptionsHelper();
        when(metadataDescriptionsHelper.getDescriptions((Set<String>) null)).thenReturn(descriptions);
        
        // Execute function under test.
        Multimap<Map.Entry<String,String>,DefaultDescription> result = dataDictionary.getDescriptions(connectionConfig);
        
        // Verify expected calls.
        verify(metadataDescriptionsHelper).initialize(accumuloClient, METADATA_TABLE, AUTHS);
        
        // Verify the result.
        Map<Map.Entry<String,String>,Collection<DefaultDescription>> resultMap = result.asMap();
        assertThat(resultMap).hasSize(3);
        assertThat(resultMap).containsEntry(Maps.immutableEntry("aliasA", DATATYPE), Collections.singleton(descriptionA));
        assertThat(resultMap).containsEntry(Maps.immutableEntry("aliasB", DATATYPE), Collections.singleton(descriptionB));
        assertThat(resultMap).containsEntry(Maps.immutableEntry("FIELDC", DATATYPE), Collections.singleton(descriptionC));
    }
    
    @Test
    public void whenRetrievingDescriptionsWithDatatype_shouldRetrieveDescriptionsWithDatatype() throws Exception {
        // Set the given field aliases.
        Map<String,String> queryModelMapping = new HashMap<>();
        queryModelMapping.put("FIELDA", "aliasA");
        queryModelMapping.put("FIELDB", "aliasB");
        givenQueryModelReverseMapping(queryModelMapping);
        givenInitializedMetadataDescriptionsHelper();
        
        // EEstablish expected result.
        DefaultDescription descriptionA = new DefaultDescription("fieldA description");
        DefaultDescription descriptionB = new DefaultDescription("fieldB description");
        DefaultDescription descriptionC = new DefaultDescription("fieldC description");
        
        SetMultimap<MetadataEntry,DefaultDescription> descriptions = HashMultimap.create();
        descriptions.put(new MetadataEntry("fieldA", DATATYPE), descriptionA);
        descriptions.put(new MetadataEntry("fieldB", DATATYPE), descriptionB);
        descriptions.put(new MetadataEntry("fieldC", DATATYPE), descriptionC);
        
        when(metadataDescriptionsHelper.getDescriptions(DATATYPE)).thenReturn(descriptions);
        
        // Execute function under test.
        Multimap<Map.Entry<String,String>,DefaultDescription> result = dataDictionary.getDescriptions(connectionConfig, DATATYPE);
        
        // Verify expected calls.
        verify(metadataDescriptionsHelper).initialize(accumuloClient, METADATA_TABLE, AUTHS);
        
        // Verify the result.
        Map<Map.Entry<String,String>,Collection<DefaultDescription>> resultMap = result.asMap();
        assertThat(resultMap).hasSize(3);
        assertThat(resultMap).containsEntry(Maps.immutableEntry("aliasA", DATATYPE), Collections.singleton(descriptionA));
        assertThat(resultMap).containsEntry(Maps.immutableEntry("aliasB", DATATYPE), Collections.singleton(descriptionB));
        assertThat(resultMap).containsEntry(Maps.immutableEntry("FIELDC", DATATYPE), Collections.singleton(descriptionC));
    }
    
    @Test
    public void whenRetrievingDescriptionsWithFieldNameAndDatatype_givenNoAlias_shouldRetrieveDescriptionsWithOriginalFieldName() throws Exception {
        // Ensure no alias will be found.
        givenQueryModelReverseMapping(FIELD_NAME, "noalias");
        givenInitializedMetadataDescriptionsHelper();
        
        // Establish expected result.
        DefaultDescription description = new DefaultDescription("description");
        when(metadataDescriptionsHelper.getDescriptions(FIELD_NAME, DATATYPE)).thenReturn(Collections.singleton(description));
        
        // Execute function under test.
        Set<DefaultDescription> descriptions = dataDictionary.getDescriptions(connectionConfig, FIELD_NAME, DATATYPE);
        
        // Verify expected calls.
        verify(metadataDescriptionsHelper).initialize(accumuloClient, METADATA_TABLE, AUTHS);
        
        // Verify the result.
        assertThat(descriptions).containsExactly(description);
    }
    
    @Test
    public void whenRetrievingDescriptionsWithFieldNameAndDatatype_givenAlias_shouldRetrieveDescriptionsWithAlias() throws Exception {
        // Ensure an alias will be found.
        givenQueryModelReverseMapping("alias", FIELD_NAME);
        givenInitializedMetadataDescriptionsHelper();
        
        // Establish expected result.
        DefaultDescription description = new DefaultDescription("description");
        when(metadataDescriptionsHelper.getDescriptions("alias", DATATYPE)).thenReturn(Collections.singleton(description));
        
        // Execute function under test.
        Set<DefaultDescription> descriptions = dataDictionary.getDescriptions(connectionConfig, FIELD_NAME, DATATYPE);
        
        // Verify expected calls.
        verify(metadataDescriptionsHelper).initialize(accumuloClient, METADATA_TABLE, AUTHS);
        
        // Verify the result.
        assertThat(descriptions).containsExactly(description);
    }
    
    @Test
    public void whenDeletingDescription_givenNoAlias_shouldDeleteDescriptionWithOriginalFieldName() throws Exception {
        Map<String,String> markings = new HashMap<>();
        markings.put("columnVisibility", "PRIVATE");
        
        DefaultDescription description = new DefaultDescription();
        description.setMarkings(markings);
        description.setDescription("my ultra cool description");
        
        // Ensure no alias will be found.
        givenQueryModelReverseMapping(FIELD_NAME, "noalias");
        givenInitializedMetadataDescriptionsHelper();
        
        // Execute function under test.
        dataDictionary.deleteDescription(connectionConfig, FIELD_NAME, DATATYPE, description);
        
        // Verify expected calls.
        verify(metadataDescriptionsHelper).initialize(accumuloClient, METADATA_TABLE, AUTHS);
        verify(metadataDescriptionsHelper).removeDescription(new MetadataEntry(FIELD_NAME, DATATYPE), description);
    }
    
    @Test
    public void whenDeletingDescription_givenAlias_shouldDeleteDescriptionWithAlias() throws Exception {
        Map<String,String> markings = new HashMap<>();
        markings.put("columnVisibility", "PRIVATE");
        
        DefaultDescription description = new DefaultDescription();
        description.setMarkings(markings);
        description.setDescription("my ultra cool description");
        
        // Ensure an alias will be found.
        givenQueryModelReverseMapping("alias", FIELD_NAME);
        givenInitializedMetadataDescriptionsHelper();
        
        // Execute function under test.
        dataDictionary.deleteDescription(connectionConfig, FIELD_NAME, DATATYPE, description);
        
        // Verify expected calls.
        verify(metadataDescriptionsHelper).initialize(accumuloClient, METADATA_TABLE, AUTHS);
        verify(metadataDescriptionsHelper).removeDescription(new MetadataEntry("alias", DATATYPE), description);
    }
    
    private void givenQueryModelReverseMapping(String key, String value) throws ExecutionException, TableNotFoundException {
        Map<String,String> map = new HashMap<>();
        map.put(key, value);
        givenQueryModelReverseMapping(map);
    }
    
    private void givenQueryModelReverseMapping(Map<String,String> map) throws ExecutionException, TableNotFoundException {
        QueryModel model = mock(QueryModel.class);
        when(model.getReverseQueryMapping()).thenReturn(map);
        
        MetadataHelper helper = mock(MetadataHelper.class);
        when(helper.getQueryModel(eq(MODEL_TABLE), eq(MODEL_NAME))).thenReturn(model);
        
        when(metadataHelperFactory.createMetadataHelper(any(), eq(METADATA_TABLE), any())).thenReturn(helper);
    }
    
    private void givenInitializedMetadataDescriptionsHelper() {
        when(metadataDescriptionsHelperFactory.createMetadataDescriptionsHelper()).thenReturn(metadataDescriptionsHelper);
    }
}
