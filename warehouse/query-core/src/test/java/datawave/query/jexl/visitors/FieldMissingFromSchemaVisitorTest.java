package datawave.query.jexl.visitors;

import static datawave.query.Constants.ANY_FIELD;
import static datawave.query.Constants.NO_FIELD;
import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.util.MockMetadataHelper;
import datawave.util.TableName;

public class FieldMissingFromSchemaVisitorTest {

    // Special fields required by visitor.
    private Set<String> specialFields = Sets.newHashSet(ANY_FIELD, NO_FIELD);
    private static final String[] AUTHS = {"FOO", "BAR"};
    private static final Set<Authorizations> AUTHS_SET = Collections.singleton(new Authorizations(AUTHS));
    private MockMetadataHelper helper;

    @Before
    public void before() throws AccumuloException, TableExistsException, AccumuloSecurityException {
        helper = new MockMetadataHelper(AUTHS_SET, AUTHS_SET, AUTHS_SET, getClient());
        if (!helper.getAccumuloClient().tableOperations().exists(TableName.METADATA)) {
            helper.getAccumuloClient().tableOperations().create(TableName.METADATA);
        }
    }

    /**
     * Test query with two field:datatype pairs that both exist in the metadata helper.
     */
    @Test
    public void testWithFieldsThatExist() throws ParseException {
        String query = "FOO == 'bar' && FOO2 == 'bar'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);

        // Fields to datatypes for our MetadataHelper
        Multimap<String,String> fieldsToDatatypes = HashMultimap.create();
        fieldsToDatatypes.put("FOO", "datatype1");
        fieldsToDatatypes.put("FOO2", "datatype2");

        // Setup MockMetadataHelper
        helper.addFieldsToDatatypes(fieldsToDatatypes);

        // Setup datatype filter
        Set<String> dataTypeFilter = Sets.newHashSet("datatype1", "datatype2");

        // Case 1 - check with a datatype filter
        checkDatatypeFilter(Collections.emptySet(), script, dataTypeFilter);

        // Case 2 - check with a null datatype filter, implies all fields.
        checkNullDatatypeFilter(Collections.emptySet(), script);

        // Case 3 - check with an empty datatype filter, implies all fields.
        checkEmptyDatatypeFilter(Collections.emptySet(), script);
    }

    /**
     * Test query with two field:datatype pairs, none of which exist in the metadata helper.
     */
    @Test
    public void testMissingFields() throws ParseException {
        String query = "FOO3 == 'bar' && FOO4 == 'bar'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);

        // Fields to datatypes for our MetadataHelper
        Multimap<String,String> fieldsToDatatypes = HashMultimap.create();
        fieldsToDatatypes.put("FOO", "datatype1");
        fieldsToDatatypes.put("FOO2", "datatype2");

        // Setup MockMetadataHelper
        helper.addFieldsToDatatypes(fieldsToDatatypes);

        // Setup datatype filter
        Set<String> dataTypeFilter = Sets.newHashSet("datatype1", "datatype2");

        // Same set expected for all three test cases.
        Set<String> expected = Sets.newHashSet("FOO3", "FOO4");

        // Case 1 - check with a datatype filter
        checkDatatypeFilter(expected, script, dataTypeFilter);

        // Case 2 - check with a null datatype filter, implies all fields.
        checkNullDatatypeFilter(expected, script);

        // Case 3 - check with an empty datatype filter, implies all fields.
        checkEmptyDatatypeFilter(expected, script);
    }

    /**
     * Test query with two field:datatype pairs, one of which does not exist in the metadta helper.
     */
    @Test
    public void testOneMissingField() throws ParseException {
        String query = "FOO == 'bar' && FOO2 == 'bar'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);

        // Fields to datatypes for our MetadataHelper
        Multimap<String,String> fieldsToDatatypes = HashMultimap.create();
        fieldsToDatatypes.put("FOO", "datatype1");

        // Setup MockMetadataHelper
        helper.addFieldsToDatatypes(fieldsToDatatypes);

        // Setup datatype filter
        Set<String> dataTypeFilter = Sets.newHashSet("datatype1", "datatype2");

        // Case 1 - check with a datatype filter
        Set<String> expected = Sets.newHashSet("FOO2");
        checkDatatypeFilter(expected, script, dataTypeFilter);

        // Case 2 - check with a null datatype filter, implies all fields.
        checkNullDatatypeFilter(expected, script);

        // Case 3 - check with an empty datatype filter, implies all fields.
        checkEmptyDatatypeFilter(expected, script);
    }

    /**
     * Test query with two field:datatype pairs, one of which is in the datatype filter.
     */
    @Test
    public void testOneMissingFieldBecauseOfDatatypeFilter() throws ParseException {
        String query = "FOO == 'bar' && FOO2 == 'bar'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);

        // Fields to datatypes for our MetadataHelper
        Multimap<String,String> fieldsToDatatypes = HashMultimap.create();
        fieldsToDatatypes.put("FOO", "datatype1");
        fieldsToDatatypes.put("FOO2", "datatype2");

        // Setup MockMetadataHelper
        helper.addFieldsToDatatypes(fieldsToDatatypes);

        // Setup datatype filter
        Set<String> dataTypeFilter = Sets.newHashSet("datatype1");

        // Case 1 - check with a datatype filter
        Set<String> expected = Sets.newHashSet("FOO2");
        checkDatatypeFilter(expected, script, dataTypeFilter);

        // Case 2 - check with a null datatype filter, implies all fields.
        checkNullDatatypeFilter(Collections.emptySet(), script);

        // Case 3 - check with an empty datatype filter, implies all fields.
        checkEmptyDatatypeFilter(Collections.emptySet(), script);
    }

    /**
     * Test query with function for a field that exists in the schema.
     */
    @Test
    public void testRegexFunctionWithKnownField() throws ParseException {
        String query = "filter:includeRegex(FOO, 'bar.*')";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);

        // Fields to datatypes for our MetadataHelper
        Multimap<String,String> fieldsToDatatypes = HashMultimap.create();
        fieldsToDatatypes.put("FOO", "datatype1");

        // Setup MockMetadataHelper
        helper.addFieldsToDatatypes(fieldsToDatatypes);

        // Setup datatype filter
        Set<String> dataTypeFilter = Sets.newHashSet("datatype1");

        // Case 1 - check with a datatype filter
        checkDatatypeFilter(Collections.emptySet(), script, dataTypeFilter);

        // Case 2 - check with a null datatype filter, implies all fields.
        checkNullDatatypeFilter(Collections.emptySet(), script);

        // Case 3 - check with an empty datatype filter, implies all fields.
        checkEmptyDatatypeFilter(Collections.emptySet(), script);
    }

    /**
     * Test query with function for a field that does not exist in the schema.
     */
    @Test
    public void testRegexFunctionWithUnknownField() throws ParseException {
        String query = "filter:includeRegex(FOO, 'bar.*')";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);

        // Setup MockMetadataHelper with emptyFieldsToDatatypes
        helper.addFieldsToDatatypes(HashMultimap.create());

        // Setup datatype filter
        Set<String> dataTypeFilter = Sets.newHashSet("datatype1");

        // Case 1 - check with a datatype filter
        Set<String> expected = Sets.newHashSet("FOO");
        checkDatatypeFilter(expected, script, dataTypeFilter);

        // Case 2 - check with a null datatype filter, implies all fields.
        checkNullDatatypeFilter(expected, script);

        // Case 3 - check with an empty datatype filter, implies all fields.
        checkEmptyDatatypeFilter(expected, script);
    }

    /**
     * Test query with function for a field that does not exist in the schema under the provided datatype filter.
     */
    @Test
    public void testRegexFunctionWithUnknownFieldBecauseOfDatatypeFilter() throws ParseException {
        String query = "filter:includeRegex(FOO, 'bar.*')";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);

        // Fields to datatypes for our MetadataHelper
        Multimap<String,String> fieldsToDatatypes = HashMultimap.create();
        fieldsToDatatypes.put("FOO", "datatype1");

        // Setup MockMetadataHelper
        helper.addFieldsToDatatypes(fieldsToDatatypes);

        // Setup datatype filter
        Set<String> dataTypeFilter = Sets.newHashSet("datatypeX");

        // Case 1 - check with a datatype filter
        Set<String> expected = Sets.newHashSet("FOO");
        checkDatatypeFilter(expected, script, dataTypeFilter);

        // Case 2 - check with a null datatype filter, implies all fields.
        checkNullDatatypeFilter(Collections.emptySet(), script);

        // Case 3 - check with an empty datatype filter, implies all fields.
        checkEmptyDatatypeFilter(Collections.emptySet(), script);
    }

    /**
     * Test query with content function.
     */
    @Test
    public void testContentFunction() throws ParseException {
        String query = "content:phrase(termOffsetMap, 'hello', 'world')";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);

        // Fields to datatypes for our MetadataHelper
        Multimap<String,String> fieldsToDatatypes = HashMultimap.create();
        fieldsToDatatypes.put("FOO", "datatype1");
        fieldsToDatatypes.put("FOO2", "datatype1");
        fieldsToDatatypes.put("BAZ", "datatype1");

        // Setup MockMetadataHelper with some indexed TF fields.
        helper.addFieldsToDatatypes(fieldsToDatatypes);
        helper.addTermFrequencyFields(Sets.newHashSet("BAR", "BAZ"));
        helper.setIndexedFields(Sets.newHashSet("BAR", "BAZ"));

        // Setup datatype filter
        Set<String> dataTypeFilter = Sets.newHashSet("datatype1");

        // Case 1 - check with a datatype filter
        Set<String> expected = Sets.newHashSet("BAR");
        checkDatatypeFilter(expected, script, dataTypeFilter);

        // Case 2 - check with a null datatype filter, implies all fields.
        checkNullDatatypeFilter(expected, script);

        // Case 3 - check with an empty datatype filter, implies all fields.
        checkEmptyDatatypeFilter(expected, script);
    }

    /**
     * Test query with content function.
     */
    @Test
    public void testContentFunctionWithOtherTerm() throws ParseException {
        String query = "content:phrase(termOffsetMap, 'hello', 'world') && FOO == 'abc'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);

        // Fields to datatypes for our MetadataHelper
        Multimap<String,String> fieldsToDatatypes = HashMultimap.create();
        fieldsToDatatypes.put("FOO", "datatypeX");
        fieldsToDatatypes.put("FOO2", "datatype1");
        fieldsToDatatypes.put("BAZ", "datatype1");

        // Setup MockMetadataHelper with some indexed TF fields.
        helper.addFieldsToDatatypes(fieldsToDatatypes);
        helper.addTermFrequencyFields(Sets.newHashSet("BAR", "BAZ"));
        helper.setIndexedFields(Sets.newHashSet("BAR", "BAZ"));

        // Setup datatype filter
        Set<String> dataTypeFilter = Sets.newHashSet("datatype1");

        // Case 1 - check with a datatype filter
        Set<String> expected = Sets.newHashSet("BAR", "FOO");
        checkDatatypeFilter(expected, script, dataTypeFilter);

        // Case 2 - check with a null datatype filter, implies all fields.
        expected = Sets.newHashSet("BAR");
        checkNullDatatypeFilter(expected, script);

        // Case 3 - check with an empty datatype filter, implies all fields.
        checkEmptyDatatypeFilter(expected, script);
    }

    private void checkDatatypeFilter(Set<String> expected, ASTJexlScript script, Set<String> dataTypeFilter) {
        Set<String> actual = FieldMissingFromSchemaVisitor.getNonExistentFields(helper, script, dataTypeFilter, specialFields);
        assertEquals(expected, actual);
    }

    private void checkNullDatatypeFilter(Set<String> expected, ASTJexlScript script) {
        Set<String> actual = FieldMissingFromSchemaVisitor.getNonExistentFields(helper, script, null, specialFields);
        assertEquals(expected, actual);
    }

    private void checkEmptyDatatypeFilter(Set<String> expected, ASTJexlScript script) {
        Set<String> actual = FieldMissingFromSchemaVisitor.getNonExistentFields(helper, script, Collections.emptySet(), specialFields);
        assertEquals(expected, actual);
    }

    private static AccumuloClient getClient() {
        try {
            return new InMemoryAccumuloClient("root", new InMemoryInstance());
        } catch (AccumuloSecurityException e) {
            throw new RuntimeException(e);
        }
    }
}
