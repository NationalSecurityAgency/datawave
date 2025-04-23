package datawave.query.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.data.MetadataCardinalityCounts;
import datawave.data.type.LcType;
import datawave.data.type.Type;
import datawave.query.composite.CompositeMetadataHelper;
import datawave.query.model.Direction;
import datawave.query.model.FieldMapping;
import datawave.query.model.ModelKeyParser;
import datawave.query.model.QueryModel;

/**
 * Integration test for the {@link MetadataHelper}.
 * <p>
 * Uses data loaded into an {@link InMemoryInstance} to drive MetadataHelper behavior.
 */
public class MetadataHelperTableTest {
    
    private static final String METADATA_TABLE_NAME = "DatawaveMetadata";
    
    private static AccumuloClient client;
    
    private MetadataHelper helper;
    private AllFieldMetadataHelper allFieldHelper;
    
    private final String[] authorizations = {"FOO", "BAR"};
    private static final Value EMPTY_VALUE = new Value();
    private static final LongCombiner.VarLenEncoder encoder = new LongCombiner.VarLenEncoder();
    
    private final Set<String> allFields = Set.of("COLOR", "SHAPE", "DEFINITION", "EVENT_ONLY");
    private final Set<String> datatypeAFields = Set.of("SHAPE", "DEFINITION", "EVENT_ONLY");
    private final Set<String> datatypeBFields = Set.of("COLOR", "DEFINITION");
    
    @BeforeAll
    public static void beforeAll() throws Exception {
        InMemoryInstance instance = new InMemoryInstance(MetadataHelperTableTest.class.getName());
        client = new InMemoryAccumuloClient("", instance);
        client.tableOperations().create(METADATA_TABLE_NAME);
        writeData();
    }
    
    private static void writeData() throws Exception {
        
        BatchWriterConfig config = new BatchWriterConfig();
        try (BatchWriter bw = client.createBatchWriter(METADATA_TABLE_NAME, config)) {
            write(bw, "SHAPE", "e", "datatype-a", EMPTY_VALUE);
            write(bw, "SHAPE", "i", "datatype-a", EMPTY_VALUE);
            write(bw, "SHAPE", "ri", "datatype-a", EMPTY_VALUE);
            write(bw, "SHAPE", "f", "datatype-a\u000020240301", createValue(23L));
            write(bw, "SHAPE", "f", "datatype-a\u000020240302", createValue(14));
            write(bw, "SHAPE", "f", "datatype-a\u000020240303", createValue(72));
            write(bw, "SHAPE", "f", "datatype-a\u000020240304", createValue(450));
            write(bw, "SHAPE", "f", "datatype-a\u000020240305", createValue(99));
            write(bw, "SHAPE", "t", "datatype-a\0" + LcType.class.getName(), EMPTY_VALUE);
            write(bw, "SHAPE", "desc", "datatype-a", new Value("a shape such as a circle, triangle, or square"));
            
            write(bw, "COLOR", "e", "datatype-b", EMPTY_VALUE);
            write(bw, "COLOR", "i", "datatype-b", EMPTY_VALUE);
            write(bw, "COLOR", "f", "datatype-b\u000020240306", createValue(66));
            write(bw, "COLOR", "f", "datatype-b\u000020240307", createValue(77));
            write(bw, "COLOR", "f", "datatype-b\u000020240308", createValue(55));
            write(bw, "COLOR", "f", "datatype-b\u000020240309", createValue(44));
            write(bw, "COLOR", "f", "datatype-b\u000020240310", createValue(213));
            write(bw, "COLOR", "t", "datatype-b\0" + LcType.class.getName(), EMPTY_VALUE);
            write(bw, "COLOR", "desc", "datatype-b", new Value("a color such as red, green or blue"));
            
            // DEFINITION is index only (no e-column)
            write(bw, "DEFINITION", "i", "datatype-a", EMPTY_VALUE);
            write(bw, "DEFINITION", "i", "datatype-b", EMPTY_VALUE);
            write(bw, "DEFINITION", "ri", "datatype-b", EMPTY_VALUE);
            write(bw, "DEFINITION", "ri", "datatype-a", EMPTY_VALUE);
            write(bw, "DEFINITION", "f", "datatype-a\u000020240301", createValue(23));
            write(bw, "DEFINITION", "f", "datatype-a\u000020240302", createValue(14));
            write(bw, "DEFINITION", "f", "datatype-a\u000020240303", createValue(72));
            write(bw, "DEFINITION", "f", "datatype-a\u000020240304", createValue(450));
            write(bw, "DEFINITION", "f", "datatype-a\u000020240305", createValue(99));
            write(bw, "DEFINITION", "f", "datatype-b\u000020240305", createValue(55));
            write(bw, "DEFINITION", "f", "datatype-b\u000020240306", createValue(66));
            write(bw, "DEFINITION", "f", "datatype-b\u000020240307", createValue(77));
            write(bw, "DEFINITION", "f", "datatype-b\u000020240308", createValue(55));
            write(bw, "DEFINITION", "f", "datatype-b\u000020240309", createValue(44));
            write(bw, "DEFINITION", "f", "datatype-b\u000020240310", createValue(213));
            write(bw, "DEFINITION", "t", "datatype-a\0" + LcType.class.getName(), EMPTY_VALUE);
            write(bw, "DEFINITION", "t", "datatype-b\0" + LcType.class.getName(), EMPTY_VALUE);
            write(bw, "DEFINITION", "desc", "datatype-a", new Value("a shape such as a circle, triangle, or square"));
            write(bw, "DEFINITION", "desc", "datatype-b", new Value("a shape such as a circle, triangle, or square"));
            write(bw, "DEFINITION", "tf", "datatype-a", EMPTY_VALUE);
            write(bw, "DEFINITION", "tf", "datatype-b", EMPTY_VALUE);
            
            // EVENT_ONLY is...event only (e column, no i, ri
            write(bw, "EVENT_ONLY", "e", "datatype-a", EMPTY_VALUE);
            write(bw, "EVENT_ONLY", "f", "datatype-a\u000020240301", createValue(23));
            write(bw, "EVENT_ONLY", "f", "datatype-a\u000020240302", createValue(14));
            write(bw, "EVENT_ONLY", "f", "datatype-a\u000020240303", createValue(72));
            write(bw, "EVENT_ONLY", "f", "datatype-a\u000020240304", createValue(450));
            write(bw, "EVENT_ONLY", "f", "datatype-a\u000020240305", createValue(99));
            write(bw, "EVENT_ONLY", "t", "datatype-a\0" + LcType.class.getName(), EMPTY_VALUE);
            write(bw, "EVENT_ONLY", "desc", "datatype-a", new Value("an event-only field, not important enough to index"));
            
            // exp column family
            write(bw, "EXP_1", "exp", "datatype-a\0", EMPTY_VALUE);
            write(bw, "EXP_2", "exp", "datatype-b\0", EMPTY_VALUE);
            
            // content column family - misconfigured on purpose, datatype-b should be included here but is not
            write(bw, "DEFINITION", "content", "datatype-a\0", EMPTY_VALUE);
            
            // write some 'counts'
            MetadataCardinalityCounts counts = new MetadataCardinalityCounts("DEFINITION", "define", 23L, 34L, 45L, 56L, 67L, 78L);
            write(bw, "DEFINITION", "count", "define", counts.getValue());
            
            // Write a model.
            bw.addMutation(ModelKeyParser.createMutation(new FieldMapping("", "EVENT_DATE", "start-time", Direction.FORWARD, "", Collections.emptySet()),
                            "TEST_MODEL"));
            bw.addMutation(ModelKeyParser.createMutation(new FieldMapping("", "EVENT_DATE", "start-time", Direction.REVERSE, "", Collections.emptySet()),
                            "TEST_MODEL"));
            bw.addMutation(ModelKeyParser.createMutation(new FieldMapping("", "UUID", "unique-id", Direction.FORWARD, "", Collections.emptySet()),
                            "TEST_MODEL"));
            bw.addMutation(ModelKeyParser.createMutation(new FieldMapping("", "UUID", "unique-id", Direction.REVERSE, "", Collections.emptySet()),
                            "TEST_MODEL"));
            // Test using regex patterns in forward matching model mappings.
            bw.addMutation(ModelKeyParser.createMutation(
                            new FieldMapping("", "TITLE|HEADER|DESIGNATION", "title", Direction.FORWARD, "", Collections.emptySet()), "TEST_MODEL"));
            // Make sure the model fields appear when fetching all fields.
            write(bw, "EVENT_DATE", "i", "datatype-a", EMPTY_VALUE);
            write(bw, "UUID", "i", "datatype-a", EMPTY_VALUE);
            write(bw, "TITLE", "i", "datatype-a", EMPTY_VALUE);
            write(bw, "HEADER", "i", "datatype-a", EMPTY_VALUE);
            write(bw, "DESIGNATION", "i", "datatype-a", EMPTY_VALUE);
        }
    }
    
    /**
     * Writes metadata information
     * 
     * @param bw
     *            the batch writer
     * @param row
     *            the row
     * @param cf
     *            the column family
     * @param cq
     *            the column qualifier
     * @param value
     *            the value
     * @throws Exception
     *             if something goes wrong
     */
    private static void write(BatchWriter bw, String row, String cf, String cq, Value value) throws Exception {
        Mutation m = new Mutation(row);
        m.put(cf, cq, value);
        bw.addMutation(m);
    }
    
    /**
     * Create a value with an encoded long
     * 
     * @param count
     *            a long
     * @return a Value with the encoded long
     */
    private static Value createValue(long count) {
        return new Value(encoder.encode(count));
    }
    
    @BeforeEach
    public void beforeEach() {
        allFieldHelper = createAllFieldMetadataHelper();
        helper = createMetadataHelper(allFieldHelper);
    }
    
    private AllFieldMetadataHelper createAllFieldMetadataHelper() {
        final Set<Authorizations> allAuths = Collections.singleton(new Authorizations(authorizations));
        final Set<Authorizations> auths = Collections.singleton(new Authorizations(authorizations));
        TypeMetadataHelper typeMetadataHelper = new TypeMetadataHelper(new HashMap<>(), auths, client, METADATA_TABLE_NAME, auths, false);
        CompositeMetadataHelper compositeMetadataHelper = new CompositeMetadataHelper(client, METADATA_TABLE_NAME, auths);
        return new AllFieldMetadataHelper(typeMetadataHelper, compositeMetadataHelper, client, METADATA_TABLE_NAME, auths, allAuths);
    }
    
    private MetadataHelper createMetadataHelper(AllFieldMetadataHelper allFieldHelper) {
        if (allFieldHelper == null) {
            allFieldHelper = createAllFieldMetadataHelper();
        }
        
        Set<Authorizations> userAuths = Collections.singleton(new Authorizations(authorizations));
        Set<Authorizations> metadataAuths = Collections.singleton(new Authorizations(authorizations));
        return new MetadataHelper(allFieldHelper, metadataAuths, client, METADATA_TABLE_NAME, userAuths, metadataAuths);
    }
    
    @Test
    public void testUserHasAllMetadataAuths() {
        final Collection<Authorizations> emptyAuths = Collections.emptySet();
        assertThrows(NoSuchElementException.class, () -> MetadataHelper.userHasAllMetadataAuths(emptyAuths, emptyAuths));
        
        final Collection<Authorizations> userAuths = Collections.singleton(new Authorizations(authorizations));
        final Collection<Authorizations> allMetadataAuths = Collections.singleton(new Authorizations(authorizations));
        assertTrue(MetadataHelper.userHasAllMetadataAuths(userAuths, allMetadataAuths));
        
        final Collection<Authorizations> subset = Collections.singleton(new Authorizations("FOO"));
        assertFalse(MetadataHelper.userHasAllMetadataAuths(subset, allMetadataAuths));
    }
    
    @Test
    public void testGetUsersMetadataAuthorizationSubset() {
        final Collection<Authorizations> userAuths = Collections.singleton(new Authorizations("FOO", "BAZ"));
        final Collection<Authorizations> allMetadataAuths = Collections.singleton(new Authorizations(authorizations));
        Collection<String> subset = MetadataHelper.getUsersMetadataAuthorizationSubset(userAuths, allMetadataAuths);
        
        Set<String> expected = Collections.singleton("FOO");
        assertEquals(expected, subset);
    }
    
    @Test
    public void testGetUsersMetadataAuthorizationSubsetAsString() {
        assertEquals("BAR&FOO", helper.getUsersMetadataAuthorizationSubset());
    }
    
    @Test
    public void testGetAllMetadataAuths() {
        Set<Authorizations> expected = Collections.singleton(new Authorizations(authorizations));
        assertEquals(expected, helper.getAllMetadataAuths());
    }
    
    @Test
    public void getAuths() {
        Set<Authorizations> expected = Collections.singleton(new Authorizations(authorizations));
        assertEquals(expected, helper.getAuths());
    }
    
    @Test
    public void getFullUserAuths() {
        Set<Authorizations> expected = Collections.singleton(new Authorizations(authorizations));
        assertEquals(expected, helper.getFullUserAuths());
    }
    
    @Test
    public void getAllFieldMetadataHelper() {
        assertEquals(allFieldHelper, helper.getAllFieldMetadataHelper());
    }
    
    @Test
    public void testGetMetadata() throws Exception {
        Metadata metadata = helper.getMetadata();
        
        Set<String> datatypes = Set.of("datatype-a", "datatype-b");
        Set<String> fields = Set.of("SHAPE", "COLOR", "DEFINITION", "EVENT_ONLY", "EVENT_DATE", "TITLE", "UUID", "HEADER", "DESIGNATION");
        Set<String> indexedFields = Set.of("SHAPE", "COLOR", "DEFINITION", "EVENT_DATE", "TITLE", "UUID", "HEADER", "DESIGNATION");
        Set<String> indexOnlyFields = Set.of("DEFINITION", "EVENT_DATE", "TITLE", "UUID", "HEADER", "DESIGNATION");
        Set<String> termFrequencyFields = Set.of("DEFINITION");
        
        assertEquals(datatypes, metadata.getDatatypes());
        assertEquals(fields, metadata.getAllFields());
        assertEquals(indexedFields, metadata.getIndexedFields());
        assertEquals(indexOnlyFields, metadata.getIndexOnlyFields());
        assertEquals(Collections.emptySet(), metadata.getNormalizedFields());
        assertEquals(termFrequencyFields, metadata.getTermFrequencyFields());
    }
    
    @Test
    public void testGetMetadataWithDatatypeFilter() throws Exception {
        Set<String> filter = Collections.singleton("datatype-a");
        Metadata metadata = helper.getMetadata(filter);
        
        Set<String> datatypes = Set.of("datatype-a");
        Set<String> fields = Set.of("SHAPE", "DEFINITION", "EVENT_ONLY", "EVENT_DATE", "TITLE", "UUID", "HEADER", "DESIGNATION");
        Set<String> indexedFields = Set.of("SHAPE", "DEFINITION", "EVENT_DATE", "TITLE", "UUID", "HEADER", "DESIGNATION");
        Set<String> indexOnlyFields = Set.of("DEFINITION", "EVENT_DATE", "TITLE", "UUID", "HEADER", "DESIGNATION");
        Set<String> termFrequencyFields = Set.of("DEFINITION");
        
        assertEquals(datatypes, metadata.getDatatypes());
        assertEquals(fields, metadata.getAllFields());
        assertEquals(indexedFields, metadata.getIndexedFields());
        assertEquals(indexOnlyFields, metadata.getIndexOnlyFields());
        assertEquals(Collections.emptySet(), metadata.getNormalizedFields());
        assertEquals(termFrequencyFields, metadata.getTermFrequencyFields());
    }
    
    @Test
    public void testGetAllFields() throws Exception {
        Set<String> expected = Set.of("SHAPE", "COLOR", "DEFINITION", "EVENT_ONLY", "EVENT_DATE", "TITLE", "UUID", "HEADER", "DESIGNATION");
        assertEquals(expected, helper.getAllFields(Collections.emptySet()));
        
        // and with filter
        Set<String> filter = Collections.singleton("datatype-a");
        expected = Set.of("SHAPE", "DEFINITION", "EVENT_ONLY", "EVENT_DATE", "TITLE", "UUID", "HEADER", "DESIGNATION");
        assertEquals(expected, helper.getAllFields(filter));
    }
    
    @Test
    public void testGetEvaluationOnlyFields() {
        Collection<String> expected = Collections.emptySet();
        assertEquals(expected, helper.getEvaluationOnlyFields());
    }
    
    @Test
    public void testGetNonEventFields() throws Exception {
        Set<String> expected = Set.of("DEFINITION", "EVENT_DATE", "HEADER", "TITLE", "UUID", "DESIGNATION");
        assertEquals(expected, helper.getNonEventFields(Collections.emptySet()));
        
        // then restrict the filter
        Set<String> filter = Collections.singleton("datatype-a");
        assertEquals(expected, helper.getNonEventFields(filter));
    }
    
    @Test
    public void testIsOverloadedCompositeField() {
        // multimap implementation
        ArrayListMultimap<String,String> composites = ArrayListMultimap.create();
        composites.putAll("A", List.of("A", "B"));
        composites.putAll("X", List.of("X", "Y"));
        
        assertTrue(MetadataHelper.isOverloadedCompositeField(composites, "A"));
        assertFalse(MetadataHelper.isOverloadedCompositeField(composites, "B"));
        
        // collection implementation
        List<String> fields = List.of("A", "B", "C");
        assertTrue(MetadataHelper.isOverloadedCompositeField(fields, "A"));
        assertFalse(MetadataHelper.isOverloadedCompositeField(fields, "B"));
    }
    
    @Test
    public void testGetIndexOnlyFields() throws Exception {
        Set<String> expected = Set.of("DEFINITION", "EVENT_DATE", "TITLE", "UUID", "HEADER", "DESIGNATION");
        assertEquals(expected, helper.getIndexOnlyFields(Collections.emptySet()));
        
        // restrict filter
        Set<String> filter = Collections.singleton("datatype-b");
        expected = Set.of("DEFINITION");
        assertEquals(expected, helper.getIndexOnlyFields(filter));
    }
    
    @Test
    public void testIsReverseIndexed() throws Exception {
        // SHAPE is reverse indexed for datatype-a
        assertTrue(helper.isReverseIndexed("SHAPE", Collections.emptySet()));
        assertTrue(helper.isReverseIndexed("SHAPE", Set.of("datatype-a")));
        assertFalse(helper.isReverseIndexed("SHAPE", Set.of("datatype-b")));
        
        // COLOR is not reverse indexed for any type
        assertFalse(helper.isReverseIndexed("COLOR", Collections.emptySet()));
        assertFalse(helper.isReverseIndexed("COLOR", Set.of("datatype-a")));
        assertFalse(helper.isReverseIndexed("COLOR", Set.of("datatype-b")));
        
        // DEFINITION is reverse indexed for both types
        assertTrue(helper.isReverseIndexed("DEFINITION", Collections.emptySet()));
        assertTrue(helper.isReverseIndexed("DEFINITION", Set.of("datatype-a")));
        assertTrue(helper.isReverseIndexed("DEFINITION", Set.of("datatype-b")));
        
        // EVENT_ONLY is not reverse indexed for any type
        assertFalse(helper.isReverseIndexed("EVENT_ONLY", Collections.emptySet()));
        assertFalse(helper.isReverseIndexed("EVENT_ONLY", Set.of("datatype-a")));
        assertFalse(helper.isReverseIndexed("EVENT_ONLY", Set.of("datatype-b")));
    }
    
    @Test
    public void isIndexed() throws Exception {
        // SHAPE is indexed for datatype-a
        assertTrue(helper.isIndexed("SHAPE", Collections.emptySet()));
        assertTrue(helper.isIndexed("SHAPE", Set.of("datatype-a")));
        assertFalse(helper.isIndexed("SHAPE", Set.of("datatype-b")));
        
        // COLOR is indexed for datatype-b
        assertTrue(helper.isIndexed("COLOR", Collections.emptySet()));
        assertFalse(helper.isIndexed("COLOR", Set.of("datatype-a")));
        assertTrue(helper.isIndexed("COLOR", Set.of("datatype-b")));
        
        // DEFINITION is indexed for both types
        assertTrue(helper.isIndexed("DEFINITION", Collections.emptySet()));
        assertTrue(helper.isIndexed("DEFINITION", Set.of("datatype-a")));
        assertTrue(helper.isIndexed("DEFINITION", Set.of("datatype-b")));
        
        // EVENT_ONLY is not indexed for any type
        assertFalse(helper.isIndexed("EVENT_ONLY", Collections.emptySet()));
        assertFalse(helper.isIndexed("EVENT_ONLY", Set.of("datatype-a")));
        assertFalse(helper.isIndexed("EVENT_ONLY", Set.of("datatype-b")));
    }
    
    @Test
    public void isTokenized() throws Exception {
        // SHAPE is indexed for datatype-a
        assertFalse(helper.isTokenized("SHAPE", Collections.emptySet()));
        assertFalse(helper.isTokenized("SHAPE", Set.of("datatype-a")));
        assertFalse(helper.isTokenized("SHAPE", Set.of("datatype-b")));
        
        // COLOR is indexed for datatype-b
        assertFalse(helper.isTokenized("COLOR", Collections.emptySet()));
        assertFalse(helper.isTokenized("COLOR", Set.of("datatype-a")));
        assertFalse(helper.isTokenized("COLOR", Set.of("datatype-b")));
        
        // DEFINITION is indexed for both types
        assertTrue(helper.isTokenized("DEFINITION", Collections.emptySet()));
        assertTrue(helper.isTokenized("DEFINITION", Set.of("datatype-a")));
        assertTrue(helper.isTokenized("DEFINITION", Set.of("datatype-b")));
        
        // EVENT_ONLY is not indexed for any type
        assertFalse(helper.isTokenized("EVENT_ONLY", Collections.emptySet()));
        assertFalse(helper.isTokenized("EVENT_ONLY", Set.of("datatype-a")));
        assertFalse(helper.isTokenized("EVENT_ONLY", Set.of("datatype-b")));
    }
    
    // skip facet stuff for now
    
    @Test
    public void testGetTermCounts() throws Exception {
        Map<String,Map<String,MetadataCardinalityCounts>> termCounts = helper.getTermCounts();
        assertTrue(termCounts.containsKey("DEFINITION"));
        
        Map<String,MetadataCardinalityCounts> valueCounts = termCounts.get("DEFINITION");
        assertTrue(valueCounts.containsKey("define"));
        
        MetadataCardinalityCounts counts = valueCounts.get("define");
        assertEquals("DEFINITION", counts.getField());
        assertEquals("define", counts.getFieldValue());
        assertEquals(23L, counts.getFieldValueCount());
        assertEquals(34L, counts.getFieldAllValueCount());
        assertEquals(45L, counts.getUniqueFieldAllValueCount());
        assertEquals(56L, counts.getTotalAllFieldAllValueCount());
        assertEquals(67L, counts.getTotalUniqueAllFieldAllValueCount());
        assertEquals(78L, counts.getTotalUniqueAllFieldCount());
    }
    
    @Test
    public void testGetTermCountsWithRootAuths() throws Exception {
        Map<String,Map<String,MetadataCardinalityCounts>> termCounts = helper.getTermCountsWithRootAuths();
        assertTrue(termCounts.containsKey("DEFINITION"));
        
        Map<String,MetadataCardinalityCounts> valueCounts = termCounts.get("DEFINITION");
        assertTrue(valueCounts.containsKey("define"));
        
        MetadataCardinalityCounts counts = valueCounts.get("define");
        assertEquals("DEFINITION", counts.getField());
        assertEquals("define", counts.getFieldValue());
        assertEquals(23L, counts.getFieldValueCount());
        assertEquals(34L, counts.getFieldAllValueCount());
        assertEquals(45L, counts.getUniqueFieldAllValueCount());
        assertEquals(56L, counts.getTotalAllFieldAllValueCount());
        assertEquals(67L, counts.getTotalUniqueAllFieldAllValueCount());
        assertEquals(78L, counts.getTotalUniqueAllFieldCount());
    }
    
    @Test
    public void testGetAllNormalized() throws Exception {
        // in practice the 'n' column family is not used
        Set<String> expected = new HashSet<>();
        assertEquals(expected, helper.getAllNormalized());
    }
    
    @Test
    public void testGetAllDatatypes() throws Exception {
        Set<Type<?>> expected = Set.of(new LcType());
        assertEquals(expected, helper.getAllDatatypes());
    }
    
    // skip compositeToFieldMap
    
    // skip whindex creation date map stuff
    
    @Test
    public void testGetDatatypesForField() throws Exception {
        Set<Type<?>> expected = Set.of(new LcType());
        assertEquals(expected, helper.getDatatypesForField("SHAPE"));
        
        assertEquals(Collections.emptySet(), helper.getDatatypesForField("FIELD_NOT_FOUND"));
        
        assertEquals(expected, helper.getDatatypesForField("SHAPE", Set.of("datatype-a")));
        assertEquals(Collections.emptySet(), helper.getDatatypesForField("SHAPE", Set.of("datatype-b")));
    }
    
    @Test
    public void testGetTypeMetadata() throws Exception {
        TypeMetadata typeMetadata = helper.getTypeMetadata();
        
        Set<String> fields = Set.of("COLOR", "SHAPE", "DEFINITION", "EVENT_ONLY");
        assertEquals(fields, typeMetadata.keySet());
        
        assertEquals(Set.of("datatype-b"), typeMetadata.getDataTypesForField("COLOR"));
        assertEquals(Set.of("datatype-a"), typeMetadata.getDataTypesForField("SHAPE"));
        assertEquals(Set.of("datatype-a", "datatype-b"), typeMetadata.getDataTypesForField("DEFINITION"));
        assertEquals(Set.of("datatype-a"), typeMetadata.getDataTypesForField("EVENT_ONLY"));
        
        assertEquals(Set.of(LcType.class.getName()), typeMetadata.getNormalizerNamesForField("COLOR"));
        assertEquals(Set.of(LcType.class.getName()), typeMetadata.getNormalizerNamesForField("SHAPE"));
        assertEquals(Set.of(LcType.class.getName()), typeMetadata.getNormalizerNamesForField("DEFINITION"));
        assertEquals(Set.of(LcType.class.getName()), typeMetadata.getNormalizerNamesForField("EVENT_ONLY"));
    }
    
    @Test
    public void testGetTypeMetadataWithFilter() throws Exception {
        TypeMetadata typeMetadata = helper.getTypeMetadata(Set.of("datatype-b"));
        
        Set<String> fields = Set.of("COLOR", "DEFINITION");
        assertEquals(fields, typeMetadata.keySet());
        
        assertEquals(Set.of("datatype-b"), typeMetadata.getDataTypesForField("COLOR"));
        assertEquals(Collections.emptySet(), typeMetadata.getDataTypesForField("SHAPE"));
        assertEquals(Set.of("datatype-b"), typeMetadata.getDataTypesForField("DEFINITION"));
        assertEquals(Collections.emptySet(), typeMetadata.getDataTypesForField("EVENT_ONLY"));
        
        assertEquals(Set.of(LcType.class.getName()), typeMetadata.getNormalizerNamesForField("COLOR"));
        assertEquals(Collections.emptySet(), typeMetadata.getNormalizerNamesForField("SHAPE"));
        assertEquals(Set.of(LcType.class.getName()), typeMetadata.getNormalizerNamesForField("DEFINITION"));
        assertEquals(Collections.emptySet(), typeMetadata.getNormalizerNamesForField("EVENT_ONLY"));
    }
    
    // skip composite metadata
    
    // skip edges
    
    @Test
    public void testGetFieldsToDatatypes() throws Exception {
        Set<Type<?>> expectedTypes = Set.of(new LcType());
        Multimap<String,Type<?>> fieldsToDatatype = helper.getFieldsToDatatypes(Collections.emptySet());
        
        assertEquals(allFields, fieldsToDatatype.keySet());
        for (String field : fieldsToDatatype.keySet()) {
            assertEquals(expectedTypes, fieldsToDatatype.get(field));
        }
        
        fieldsToDatatype = helper.getFieldsToDatatypes(Set.of("datatype-a"));
        assertEquals(datatypeAFields, fieldsToDatatype.keySet());
        for (String field : fieldsToDatatype.keySet()) {
            assertEquals(expectedTypes, fieldsToDatatype.get(field));
        }
        
        fieldsToDatatype = helper.getFieldsToDatatypes(Set.of("datatype-b"));
        assertEquals(datatypeBFields, fieldsToDatatype.keySet());
        for (String field : fieldsToDatatype.keySet()) {
            assertEquals(expectedTypes, fieldsToDatatype.get(field));
        }
    }
    
    @Test
    public void testGetFieldsForDatatype() throws Exception {
        Class<? extends Type<?>> type = LcType.class;
        assertEquals(allFields, helper.getFieldsForDatatype(type, null));
        assertEquals(allFields, helper.getFieldsForDatatype(type, Collections.emptySet()));
        
        assertEquals(datatypeAFields, helper.getFieldsForDatatype(type, Set.of("datatype-a")));
        assertEquals(datatypeBFields, helper.getFieldsForDatatype(type, Set.of("datatype-b")));
    }
    
    @Test
    public void testGetDatatypeFromClass() throws Exception {
        Class<? extends Type<?>> type = LcType.class;
        assertEquals(new LcType(), helper.getDatatypeFromClass(type));
    }
    
    @Test
    public void testGetTermFrequencyFields() throws Exception {
        Set<String> tokenizedFields = Set.of("DEFINITION");
        assertEquals(tokenizedFields, helper.getTermFrequencyFields(null));
        assertEquals(tokenizedFields, helper.getTermFrequencyFields(Collections.emptySet()));
        assertEquals(Collections.emptySet(), helper.getTermFrequencyFields(Set.of("datatype-c")));
    }
    
    @Test
    public void testGetIndexedFields() throws Exception {
        Set<String> indexedFields = Set.of("SHAPE", "COLOR", "DEFINITION", "EVENT_DATE", "DESIGNATION", "HEADER", "TITLE", "UUID");
        assertEquals(indexedFields, helper.getIndexedFields(null));
        assertEquals(indexedFields, helper.getIndexedFields(Collections.emptySet()));
        assertEquals(Set.of("SHAPE", "DEFINITION", "EVENT_DATE", "TITLE", "UUID", "HEADER", "DESIGNATION"), helper.getIndexedFields(Set.of("datatype-a")));
        assertEquals(Set.of("COLOR", "DEFINITION"), helper.getIndexedFields(Set.of("datatype-b")));
        assertEquals(Collections.emptySet(), helper.getIndexedFields(Set.of("datatype-c")));
    }
    
    @Test
    public void testGetReverseIndexedFields() throws Exception {
        Set<String> reverseIndexedFields = Set.of("SHAPE", "DEFINITION");
        assertEquals(reverseIndexedFields, helper.getReverseIndexedFields(null));
        assertEquals(reverseIndexedFields, helper.getReverseIndexedFields(Collections.emptySet()));
        assertEquals(Set.of("SHAPE", "DEFINITION"), helper.getReverseIndexedFields(Set.of("datatype-a")));
        assertEquals(Set.of("DEFINITION"), helper.getReverseIndexedFields(Set.of("datatype-b")));
        assertEquals(Collections.emptySet(), helper.getReverseIndexedFields(Set.of("datatype-c")));
    }
    
    @Test
    public void testGetExpansionFields() throws Exception {
        Set<String> expansionFields = Set.of("EXP_1", "EXP_2");
        assertEquals(expansionFields, helper.getExpansionFields(null));
        assertEquals(expansionFields, helper.getExpansionFields(Collections.emptySet()));
        assertEquals(Set.of("EXP_1"), helper.getExpansionFields(Set.of("datatype-a")));
        assertEquals(Set.of("EXP_2"), helper.getExpansionFields(Set.of("datatype-b")));
        assertEquals(Collections.emptySet(), helper.getExpansionFields(Set.of("datatype-c")));
    }
    
    @Test
    public void testGetContentFields() throws Exception {
        Set<String> contentFields = Set.of("DEFINITION");
        assertEquals(contentFields, helper.getContentFields(null));
        assertEquals(contentFields, helper.getContentFields(Collections.emptySet()));
        assertEquals(contentFields, helper.getContentFields(Set.of("datatype-a")));
        assertEquals(Collections.emptySet(), helper.getContentFields(Set.of("datatype-b")));
    }
    
    @Test
    public void testGetCardinalityForField() throws Exception {
        // full date range
        assertEquals(658L, helper.getCardinalityForField("SHAPE", getDate("20240301"), getDate("20240305")));
        
        // partial date range
        assertEquals(109L, helper.getCardinalityForField("SHAPE", getDate("20240301"), getDate("20240303")));
        
        // date range that is single day
        assertEquals(23L, helper.getCardinalityForField("SHAPE", getDate("20240301"), getDate("20240301")));
        
        // date range fully outside what's loaded
        assertEquals(0L, helper.getCardinalityForField("SHAPE", getDate("20240306"), getDate("20240310")));
        
        // date range that starts before the first loaded (should hit first three days)
        assertEquals(109L, helper.getCardinalityForField("SHAPE", getDate("20240215"), getDate("20240303")));
        
        // date range that ends after the last loaded (last two days)
        assertEquals(549L, helper.getCardinalityForField("SHAPE", getDate("20240304"), getDate("20240321")));
        
        // field that does not have an 'f' column
        assertEquals(0L, helper.getCardinalityForField("EXP_1", getDate("20240301"), getDate("20240321")));
    }
    
    @Test
    public void testGetCardinalityForFieldWithIngestTypeFilter() throws Exception {
        
        // ingest type filter match
        assertEquals(23L, helper.getCardinalityForField("SHAPE", "datatype-a", getDate("20240301"), getDate("20240301")));
        
        // ingest type filter miss
        assertEquals(0L, helper.getCardinalityForField("SHAPE", "datatype-b", getDate("20240301"), getDate("20240301")));
    }
    
    @Test
    public void testGetDatatypes() throws Exception {
        Set<String> expectedDatatypes = Set.of("datatype-a", "datatype-b");
        assertEquals(expectedDatatypes, helper.getDatatypes(null));
        assertEquals(expectedDatatypes, helper.getDatatypes(Collections.emptySet()));
        assertEquals(expectedDatatypes, helper.getDatatypes(expectedDatatypes));
        
        // sub sets
        assertEquals(Set.of("datatype-a"), helper.getDatatypes(Set.of("datatype-a")));
        assertEquals(Set.of("datatype-b"), helper.getDatatypes(Set.of("datatype-b")));
        
        // super sets
        assertEquals(Set.of("datatype-a"), helper.getDatatypes(Set.of("datatype-a", "datatype-c")));
        assertEquals(Set.of("datatype-b"), helper.getDatatypes(Set.of("datatype-b", "datatype-c")));
        
        // exclusive sets
        assertEquals(Collections.emptySet(), helper.getDatatypes(Set.of("datatype-x", "datatype-y")));
    }
    
    @Test
    public void testGetCountsByFieldForDays() {
        // range of single day
        assertEquals(23L, helper.getCountsByFieldForDays("SHAPE", getDate("20240301"), getDate("20240301")));
        // range of first two days -- only counts the first day
        assertEquals(23L, helper.getCountsByFieldForDays("SHAPE", getDate("20240301"), getDate("20240302")));
        // range out of bounds
        assertEquals(0L, helper.getCountsByFieldForDays("SHAPE", getDate("20240202"), getDate("20240203")));
    }
    
    @Test
    public void testGetCountsByFieldForDaysWithIngestTypeFilter() {
        // range of single day
        assertThrows(NullPointerException.class, () -> helper.getCountsByFieldForDays("SHAPE", getDate("20240301"), getDate("20240301"), null));
        assertEquals(0L, helper.getCountsByFieldForDays("SHAPE", getDate("20240301"), getDate("20240301"), Collections.emptySet()));
        assertEquals(23L, helper.getCountsByFieldForDays("SHAPE", getDate("20240301"), getDate("20240301"), Set.of("datatype-a")));
        assertEquals(0L, helper.getCountsByFieldForDays("SHAPE", getDate("20240301"), getDate("20240301"), Set.of("datatype-b")));
        
        // full range
        assertThrows(NullPointerException.class, () -> helper.getCountsByFieldForDays("SHAPE", getDate("20240301"), getDate("20240305"), null));
        assertEquals(0L, helper.getCountsByFieldForDays("SHAPE", getDate("20240301"), getDate("20240305"), Collections.emptySet()));
        // 559 is wrong. Full count is 658.
        assertEquals(559L, helper.getCountsByFieldForDays("SHAPE", getDate("20240301"), getDate("20240305"), Set.of("datatype-a")));
        assertEquals(0L, helper.getCountsByFieldForDays("SHAPE", getDate("20240301"), getDate("20240305"), Set.of("datatype-b")));
    }
    
    @Test
    public void testGetCountsByFieldInDay() {
        assertEquals(23L, helper.getCountsByFieldInDay("SHAPE", "20240301"));
        assertEquals(0L, helper.getCountsByFieldInDay("SHAPE", "20240315"));
    }
    
    @Test
    public void testGetCountsByFieldInDayWithTypes() {
        assertThrows(NullPointerException.class, () -> helper.getCountsByFieldInDayWithTypes("SHAPE", "20240301", null));
        assertEquals(0L, helper.getCountsByFieldInDayWithTypes("SHAPE", "20240301", Collections.emptySet()));
        assertEquals(23L, helper.getCountsByFieldInDayWithTypes("SHAPE", "20240301", Set.of("datatype-a")));
        assertEquals(0L, helper.getCountsByFieldInDayWithTypes("SHAPE", "20240315", Set.of("datatype-a")));
    }
    
    // test the 'getCountsByFieldInDayWithTypes' method that accepts an 'Entry<String, String>' as an argument
    @Test
    public void testGetCountsByFieldInDayWithTypesViaEntry() throws Exception {
        Entry<String,String> entry = new AbstractMap.SimpleEntry<>("SHAPE", "20240301");
        Map<String,Long> map = helper.getCountsByFieldInDayWithTypes(entry);
        Map<String,Long> expected = Collections.singletonMap("datatype-a", 23L);
        assertEquals(expected, map);
        
        entry = new AbstractMap.SimpleEntry<>("DEFINITION", "20240305");
        map = helper.getCountsByFieldInDayWithTypes(entry);
        expected = new HashMap<>();
        expected.put("datatype-a", 99L);
        expected.put("datatype-b", 55L);
        assertEquals(expected, map);
    }
    
    // skip getCountsByFieldInDayWithTypes(String fieldName, String date, AccumuloClient client, WrappedAccumuloClient wrappedClient)
    
    @Test
    public void testGetEarliestOccurrenceOfField() {
        assertEquals(getDate("20240301"), helper.getEarliestOccurrenceOfField("SHAPE"));
        assertEquals(getDate("20240306"), helper.getEarliestOccurrenceOfField("COLOR"));
        assertEquals(getDate("20240301"), helper.getEarliestOccurrenceOfField("DEFINITION"));
        assertEquals(getDate("20240301"), helper.getEarliestOccurrenceOfField("EVENT_ONLY"));
        assertNull(helper.getEarliestOccurrenceOfField("EXP_1"));
    }
    
    @Test
    public void testGetEarliestOccurrenceOfFieldWithType() {
        // datatype-a
        assertEquals(getDate("20240301"), helper.getEarliestOccurrenceOfFieldWithType("SHAPE", "datatype-a"));
        assertNull(helper.getEarliestOccurrenceOfFieldWithType("COLOR", "datatype-a")); // COLOR is not found in datatype-a
        assertEquals(getDate("20240301"), helper.getEarliestOccurrenceOfFieldWithType("DEFINITION", "datatype-a"));
        assertEquals(getDate("20240301"), helper.getEarliestOccurrenceOfFieldWithType("EVENT_ONLY", "datatype-a"));
        assertNull(helper.getEarliestOccurrenceOfFieldWithType("EXP_1", "datatype-a"));
        
        // datatype-b
        assertNull(helper.getEarliestOccurrenceOfFieldWithType("SHAPE", "datatype-b")); // SHAPE is not found in datatype-b
        assertEquals(getDate("20240306"), helper.getEarliestOccurrenceOfFieldWithType("COLOR", "datatype-b"));
        assertEquals(getDate("20240305"), helper.getEarliestOccurrenceOfFieldWithType("DEFINITION", "datatype-b"));
        assertNull(helper.getEarliestOccurrenceOfFieldWithType("EVENT_ONLY", "datatype-b")); // EVENT_ONLY is not found in datatype-b
        assertNull(helper.getEarliestOccurrenceOfFieldWithType("EXP_1", "datatype-b"));
    }
    
    // skip getEarliestOccurrenceOfFieldWithType(String fieldName, final String dataType, AccumuloClient client, WrappedAccumuloClient wrappedClient)
    
    // skip updateCache
    
    @Test
    public void testFieldNamesFunction() {
        List<MetadataEntry> metadataEntries = new ArrayList<>();
        metadataEntries.add(new MetadataEntry("FIELD1", "datatype-1"));
        metadataEntries.add(new MetadataEntry("FIELD1", "datatype-2"));
        
        Iterable<String> names = MetadataHelper.fieldNames(metadataEntries);
        assertIterable(List.of("FIELD1", "FIELD1"), names);
    }
    
    @Test
    public void testUniqueFieldNamesFunction() {
        List<MetadataEntry> metadataEntries = new ArrayList<>();
        metadataEntries.add(new MetadataEntry("FIELD1", "datatype-1"));
        metadataEntries.add(new MetadataEntry("FIELD1", "datatype-2"));
        
        Iterable<String> names = MetadataHelper.uniqueFieldNames(metadataEntries);
        assertIterable(List.of("FIELD1"), names);
    }
    
    @Test
    public void testDatatypesFunction() {
        List<MetadataEntry> metadataEntries = new ArrayList<>();
        metadataEntries.add(new MetadataEntry("FIELD1", "datatype-1"));
        metadataEntries.add(new MetadataEntry("FIELD2", "datatype-1"));
        
        Iterable<String> names = MetadataHelper.datatypes(metadataEntries);
        assertIterable(List.of("datatype-1", "datatype-1"), names);
    }
    
    @Test
    public void testUniqueDatatypesFunction() {
        List<MetadataEntry> metadataEntries = new ArrayList<>();
        metadataEntries.add(new MetadataEntry("FIELD1", "datatype-1"));
        metadataEntries.add(new MetadataEntry("FIELD2", "datatype-1"));
        
        Iterable<String> names = MetadataHelper.uniqueDatatypes(metadataEntries);
        assertIterable(List.of("datatype-1"), names);
    }
    
    @Test
    public void testLoadAllFields() throws Exception {
        Multimap<String,String> fields = helper.loadAllFields();
        assertEquals(Set.of("datatype-a", "datatype-b"), fields.keySet());
        assertEquals(Set.of("SHAPE", "DEFINITION", "EVENT_ONLY", "EVENT_DATE", "TITLE", "UUID", "HEADER", "DESIGNATION"), fields.get("datatype-a"));
        assertEquals(Set.of("COLOR", "DEFINITION"), fields.get("datatype-b"));
    }
    
    @Test
    public void testLoadIndexOnlyFields() throws Exception {
        Multimap<String,String> fields = helper.loadIndexOnlyFields();
        assertEquals(Set.of("datatype-a", "datatype-b"), fields.keySet());
        assertEquals(Set.of("DEFINITION", "EVENT_DATE", "TITLE", "UUID", "HEADER", "DESIGNATION"), fields.get("datatype-a"));
        assertEquals(Set.of("DEFINITION"), fields.get("datatype-b"));
    }
    
    @Test
    public void testLoadTermFrequencyFields() throws Exception {
        Multimap<String,String> fields = helper.loadTermFrequencyFields();
        assertEquals(Set.of("datatype-a", "datatype-b"), fields.keySet());
        assertEquals(Set.of("DEFINITION"), fields.get("datatype-a"));
        assertEquals(Set.of("DEFINITION"), fields.get("datatype-b"));
    }
    
    // skip basicIterator that logs the full table
    
    @Test
    public void testGetMetadataTableName() {
        assertEquals(METADATA_TABLE_NAME, helper.getMetadataTableName());
    }
    
    @Test
    public void testGetCountsForFieldsInDateRange_SingleFieldSingleDatatypeSingleDayRange() {
        // presuming a query like "SHAPE == 'foo' && COLOR == 'bar'"
        Set<String> fields = Set.of("SHAPE");
        Set<String> datatypes = Set.of("datatype-a");
        Map<String,Long> counts = helper.getCountsForFieldsInDateRange(fields, datatypes, "20240302", "20240302");
        
        assertTrue(counts.containsKey("SHAPE"));
        assertEquals(14L, counts.get("SHAPE"));
    }
    
    @Test
    public void testGetCountsForFieldsInDateRange_MultiFieldMultiDatatypeSingleDayRange() {
        // presuming a query like "SHAPE == 'foo' && COLOR == 'bar'"
        Set<String> fields = Set.of("SHAPE", "DEFINITION");
        Set<String> datatypes = Set.of("datatype-a", "datatype-b");
        Map<String,Long> counts = helper.getCountsForFieldsInDateRange(fields, datatypes, "20240302", "20240302");
        
        assertTrue(counts.containsKey("SHAPE"));
        assertTrue(counts.containsKey("DEFINITION"));
        assertEquals(14L, counts.get("SHAPE"));
        assertEquals(14L, counts.get("DEFINITION"));
    }
    
    @Test
    public void testGetCountsForFieldsInDateRange_SingleFieldNoDatatypesMultiDayRange() {
        // presuming a query like "SHAPE == 'foo' && COLOR == 'bar'"
        Set<String> fields = Set.of("SHAPE");
        Map<String,Long> counts = helper.getCountsForFieldsInDateRange(fields, Set.of(), "20240302", "20240304");
        
        assertTrue(counts.containsKey("SHAPE"));
        assertEquals(536L, counts.get("SHAPE"));
    }
    
    @Test
    public void testInternalTypeCache() throws TableNotFoundException, InstantiationException, IllegalAccessException {
        MetadataHelper helperWithDefaultCache = createMetadataHelper(null);
        
        // repeated calls to method that returns Types should return the same objects
        Set<Type<?>> typesFirstCall = helperWithDefaultCache.getAllDatatypes();
        Set<Type<?>> typesSecondCall = helperWithDefaultCache.getAllDatatypes();
        
        assertEquals(1, typesFirstCall.size());
        assertEquals(1, typesSecondCall.size());
        assertSame(typesFirstCall.iterator().next(), typesSecondCall.iterator().next());
        
        MetadataHelper helperWithConfiguredCache = createMetadataHelper(null);
        helperWithConfiguredCache.setTypeCacheSize(1);
        helperWithConfiguredCache.setTypeCacheExpirationInMinutes(0);
        
        // lowering the cache size and making repeated calls to methods that return Types should return different objects
        typesFirstCall = helperWithConfiguredCache.getAllDatatypes();
        typesSecondCall = helperWithConfiguredCache.getAllDatatypes();
        
        assertEquals(1, typesFirstCall.size());
        assertEquals(1, typesSecondCall.size());
        assertNotSame(typesFirstCall.iterator().next(), typesSecondCall.iterator().next());
    }
    
    @Test
    public void testGetQueryModel() throws TableNotFoundException, ExecutionException {
        QueryModel queryModel = helper.getQueryModel(METADATA_TABLE_NAME, "TEST_MODEL");
        
        // Assert the forward mappings.
        Multimap<String,String> forwardMappings = queryModel.getForwardQueryMapping();
        Assertions.assertTrue(forwardMappings.containsEntry("start-time", "EVENT_DATE"));
        Assertions.assertTrue(forwardMappings.containsEntry("unique-id", "UUID"));
        Assertions.assertTrue(forwardMappings.containsEntry("title", "TITLE"));
        Assertions.assertTrue(forwardMappings.containsEntry("title", "HEADER"));
        Assertions.assertTrue(forwardMappings.containsEntry("title", "DESIGNATION"));
        
        // Assert the reverse mappings.
        Map<String,String> reverseMappings = queryModel.getReverseQueryMapping();
        Assertions.assertEquals(reverseMappings.get("EVENT_DATE"), "start-time");
        Assertions.assertEquals(reverseMappings.get("UUID"), "unique-id");
    }
    
    /**
     * Assert that an iterable matches expectations
     * 
     * @param expected
     *            the expected values
     * @param iterable
     *            an Iterable of values
     */
    private void assertIterable(List<String> expected, Iterable<String> iterable) {
        Iterator<String> iter = iterable.iterator();
        for (String expect : expected) {
            assertEquals(expect, iter.next());
        }
        assertFalse(iter.hasNext());
    }
    
    private static final SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
    
    /**
     * Parse a string representation of a date in yyyyMMdd format into a {@link Date}.
     * 
     * @param time
     *            the date in string form
     * @return the Date
     */
    private Date getDate(String time) {
        try {
            return formatter.parse(time);
        } catch (ParseException e) {
            fail("Failed to parse date: " + time);
        }
        return null;
    }
    
}
