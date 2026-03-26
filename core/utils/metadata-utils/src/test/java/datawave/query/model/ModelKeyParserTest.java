package datawave.query.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ModelKeyParserTest {

    private static final String MODEL_NAME = "MODEL";
    private static final String FIELD_NAME = "field1";
    private static final String MODEL_FIELD_NAME = "mappedField1";
    private static final String DATATYPE = "test";
    private static final String COLVIZ = "PRIVATE";
    private static final Direction FORWARD = Direction.FORWARD;
    private static final Direction REVERSE = Direction.REVERSE;
    private static final Set<Authorizations> AUTHS = Collections.singleton(new Authorizations("PRIVATE, PUBLIC"));
    private static FieldMapping FORWARD_FIELD_MAPPING = null;
    private static FieldMapping STRICT_MAPPING = null;
    private static FieldMapping REVERSE_FIELD_MAPPING = null;
    private static FieldMapping NULL_CV_MAPPING = null;
    private static FieldMapping VERSION_MAPPING = null;
    private static Key FORWARD_KEY = null;
    private static Key STRICT_KEY = null;
    private static Key REVERSE_KEY = null;
    private static Key NULL_CV_KEY = null;
    private static Key VERSION_KEY1 = null;
    private static Value VERSION_VALUE1 = null;
    private static Key VERSION_KEY2 = null;
    private static Value VERSION_VALUE2 = null;
    private static Key VERSION_KEY3 = null;
    private static Value VERSION_VALUE3 = null;
    private static Mutation FORWARD_MUTATION = null;
    private static Mutation FORWARD_DELETE_MUTATION = null;
    private static Mutation STRICT_MUTATION = null;
    private static Mutation STRICT_DELETE_MUTATION = null;
    private static Mutation REVERSE_MUTATION = null;
    private static Mutation REVERSE_DELETE_MUTATION = null;

    private static final long TIMESTAMP = System.currentTimeMillis();

    @BeforeEach
    public void setup() {
        ModelKeyParser.clock = () -> TIMESTAMP;
        FORWARD_FIELD_MAPPING = new FieldMapping();
        FORWARD_FIELD_MAPPING.setColumnVisibility(COLVIZ);
        FORWARD_FIELD_MAPPING.setDatatype(DATATYPE);
        FORWARD_FIELD_MAPPING.setDirection(FORWARD);
        FORWARD_FIELD_MAPPING.setFieldName(FIELD_NAME);
        FORWARD_FIELD_MAPPING.setModelFieldName(MODEL_FIELD_NAME);
        STRICT_MAPPING = new FieldMapping();
        STRICT_MAPPING.setColumnVisibility(COLVIZ);
        STRICT_MAPPING.setDatatype(DATATYPE);
        STRICT_MAPPING.setModelFieldName(MODEL_FIELD_NAME);
        STRICT_MAPPING.addAttribute(QueryModel.STRICT);
        REVERSE_FIELD_MAPPING = new FieldMapping();
        REVERSE_FIELD_MAPPING.setColumnVisibility(COLVIZ);
        REVERSE_FIELD_MAPPING.setDatatype(DATATYPE);
        REVERSE_FIELD_MAPPING.setDirection(REVERSE);
        REVERSE_FIELD_MAPPING.setFieldName(FIELD_NAME);
        REVERSE_FIELD_MAPPING.setModelFieldName(MODEL_FIELD_NAME);
        NULL_CV_MAPPING = new FieldMapping();
        NULL_CV_MAPPING.setColumnVisibility("");
        NULL_CV_MAPPING.setDatatype(DATATYPE);
        NULL_CV_MAPPING.setDirection(REVERSE);
        NULL_CV_MAPPING.setFieldName(FIELD_NAME);
        NULL_CV_MAPPING.setModelFieldName(MODEL_FIELD_NAME);
        VERSION_MAPPING = new FieldMapping();
        VERSION_MAPPING.setColumnVisibility(COLVIZ);
        VERSION_MAPPING.addAttribute("version=VER");
        FORWARD_KEY = new Key(MODEL_FIELD_NAME, MODEL_NAME + ModelKeyParser.NULL_BYTE + DATATYPE, FIELD_NAME + ModelKeyParser.NULL_BYTE + FORWARD.getValue(),
                        COLVIZ, TIMESTAMP);
        STRICT_KEY = new Key(MODEL_FIELD_NAME, MODEL_NAME + ModelKeyParser.NULL_BYTE + DATATYPE, QueryModel.STRICT, COLVIZ, TIMESTAMP);
        REVERSE_KEY = new Key(FIELD_NAME, MODEL_NAME + ModelKeyParser.NULL_BYTE + DATATYPE, MODEL_FIELD_NAME + ModelKeyParser.NULL_BYTE + REVERSE.getValue(),
                        COLVIZ, TIMESTAMP);
        NULL_CV_KEY = new Key(FIELD_NAME, MODEL_NAME + ModelKeyParser.NULL_BYTE + DATATYPE, MODEL_FIELD_NAME + ModelKeyParser.NULL_BYTE + REVERSE.getValue(),
                        "", TIMESTAMP);
        VERSION_KEY1 = new Key(ModelKeyParser.MODEL, MODEL_NAME, "attrs", COLVIZ, TIMESTAMP);
        VERSION_VALUE1 = new Value("version=VER");
        VERSION_KEY2 = new Key(ModelKeyParser.MODEL, MODEL_NAME, "version", COLVIZ, TIMESTAMP);
        VERSION_VALUE2 = new Value("VER");
        VERSION_KEY3 = new Key(ModelKeyParser.MODEL, MODEL_NAME, "version=VER", COLVIZ, TIMESTAMP);
        VERSION_VALUE3 = new Value("");
        FORWARD_MUTATION = new Mutation(MODEL_FIELD_NAME);
        FORWARD_MUTATION.put(MODEL_NAME + ModelKeyParser.NULL_BYTE + DATATYPE, FIELD_NAME + ModelKeyParser.NULL_BYTE + FORWARD.getValue(),
                        new ColumnVisibility(COLVIZ), TIMESTAMP, ModelKeyParser.NULL_VALUE);
        FORWARD_DELETE_MUTATION = new Mutation(MODEL_FIELD_NAME);
        FORWARD_DELETE_MUTATION.putDelete(MODEL_NAME + ModelKeyParser.NULL_BYTE + DATATYPE, FIELD_NAME + ModelKeyParser.NULL_BYTE + FORWARD.getValue(),
                        new ColumnVisibility(COLVIZ), TIMESTAMP);
        FORWARD_DELETE_MUTATION.putDelete(MODEL_NAME + ModelKeyParser.NULL_BYTE + DATATYPE,
                        FIELD_NAME + ModelKeyParser.NULL_BYTE + "index_only" + ModelKeyParser.NULL_BYTE + FORWARD.getValue(), new ColumnVisibility(COLVIZ),
                        TIMESTAMP);
        STRICT_MUTATION = new Mutation(MODEL_FIELD_NAME);
        STRICT_MUTATION.put(MODEL_NAME + ModelKeyParser.NULL_BYTE + DATATYPE, QueryModel.STRICT, new ColumnVisibility(COLVIZ), TIMESTAMP,
                        ModelKeyParser.NULL_VALUE);
        STRICT_DELETE_MUTATION = new Mutation(MODEL_FIELD_NAME);
        STRICT_DELETE_MUTATION.putDelete(MODEL_NAME + ModelKeyParser.NULL_BYTE + DATATYPE, ModelKeyParser.ATTRIBUTES, new ColumnVisibility(COLVIZ), TIMESTAMP);
        STRICT_DELETE_MUTATION.putDelete(MODEL_NAME + ModelKeyParser.NULL_BYTE + DATATYPE, QueryModel.STRICT, new ColumnVisibility(COLVIZ), TIMESTAMP);

        REVERSE_MUTATION = new Mutation(FIELD_NAME);
        REVERSE_MUTATION.put(MODEL_NAME + ModelKeyParser.NULL_BYTE + DATATYPE, MODEL_FIELD_NAME + ModelKeyParser.NULL_BYTE + REVERSE.getValue(),
                        new ColumnVisibility(COLVIZ), TIMESTAMP, ModelKeyParser.NULL_VALUE);
        REVERSE_DELETE_MUTATION = new Mutation(FIELD_NAME);
        REVERSE_DELETE_MUTATION.putDelete(MODEL_NAME + ModelKeyParser.NULL_BYTE + DATATYPE, MODEL_FIELD_NAME + ModelKeyParser.NULL_BYTE + REVERSE.getValue(),
                        new ColumnVisibility(COLVIZ), TIMESTAMP);
    }

    @AfterEach
    public void tearDown() {
        ModelKeyParser.clock = System::currentTimeMillis;
    }

    @Test
    public void testForwardKeyParse() {
        FieldMapping mapping = ModelKeyParser.parseKey(FORWARD_KEY);
        assertEquals(FORWARD_FIELD_MAPPING, mapping);

        // Test ForwardKeyParse with no datatype
        FORWARD_FIELD_MAPPING.setDatatype(null);
        FORWARD_KEY = new Key(MODEL_FIELD_NAME, MODEL_NAME, FIELD_NAME + ModelKeyParser.NULL_BYTE + FORWARD.getValue(), COLVIZ, TIMESTAMP);

        mapping = ModelKeyParser.parseKey(FORWARD_KEY);
        assertEquals(FORWARD_FIELD_MAPPING, mapping);
    }

    @Test
    public void testStrictKeyParse() {
        FieldMapping mapping = ModelKeyParser.parseKey(STRICT_KEY);
        assertEquals(STRICT_MAPPING, mapping);

        // Test ForwardKeyParse with no datatype
        STRICT_MAPPING.setDatatype(null);
        STRICT_KEY = new Key(MODEL_FIELD_NAME, MODEL_NAME, QueryModel.STRICT, COLVIZ, TIMESTAMP);

        mapping = ModelKeyParser.parseKey(STRICT_KEY);
        assertEquals(STRICT_MAPPING, mapping);
    }

    @Test
    public void testVersionKeyParse() {
        FieldMapping mapping = ModelKeyParser.parseKey(VERSION_KEY1, VERSION_VALUE1);
        assertEquals(VERSION_MAPPING, mapping);
        mapping = ModelKeyParser.parseKey(VERSION_KEY2, VERSION_VALUE2);
        assertEquals(VERSION_MAPPING, mapping);
        mapping = ModelKeyParser.parseKey(VERSION_KEY3, VERSION_VALUE3);
        assertEquals(VERSION_MAPPING, mapping);
    }

    @Test
    public void testReverseKeyParse() {
        FieldMapping mapping = ModelKeyParser.parseKey(REVERSE_KEY);
        assertEquals(REVERSE_FIELD_MAPPING, mapping);

        // Test ReverseKeyParse with no datatype
        REVERSE_FIELD_MAPPING.setDatatype(null);
        REVERSE_KEY = new Key(FIELD_NAME, MODEL_NAME, MODEL_FIELD_NAME + ModelKeyParser.NULL_BYTE + REVERSE.getValue(), COLVIZ, TIMESTAMP);
        mapping = ModelKeyParser.parseKey(REVERSE_KEY);
        assertEquals(REVERSE_FIELD_MAPPING, mapping, "ReverseKeyParse with no datatype failed.");
    }

    @Test
    public void testForwardMappingParse() {
        Key k = ModelKeyParser.createKey(FORWARD_FIELD_MAPPING, MODEL_NAME);
        assertEquals(FORWARD_KEY, k);

        // Test forwardMappingParse with null datatype
        FORWARD_FIELD_MAPPING.setDatatype(null);
        k = ModelKeyParser.createKey(FORWARD_FIELD_MAPPING, MODEL_NAME);
        FORWARD_KEY = new Key(MODEL_FIELD_NAME, MODEL_NAME, FIELD_NAME + ModelKeyParser.NULL_BYTE + FORWARD.getValue(), COLVIZ, TIMESTAMP);
        assertEquals(FORWARD_KEY, k);
    }

    @Test
    public void testStrictMappingParse() {
        Key k = ModelKeyParser.createKey(STRICT_MAPPING, MODEL_NAME);
        assertEquals(STRICT_KEY, k);

        // Test StrictMappingParse with null datatype
        STRICT_MAPPING.setDatatype(null);
        k = ModelKeyParser.createKey(STRICT_MAPPING, MODEL_NAME);
        STRICT_KEY = new Key(MODEL_FIELD_NAME, MODEL_NAME, QueryModel.STRICT, COLVIZ, TIMESTAMP);
        assertEquals(STRICT_KEY, k);
    }

    @Test
    public void testReverseMappingParse() {
        Key k = ModelKeyParser.createKey(REVERSE_FIELD_MAPPING, MODEL_NAME);
        assertEquals(REVERSE_KEY, k);

        // Test with null datatype
        REVERSE_FIELD_MAPPING.setDatatype(null);
        REVERSE_KEY = new Key(FIELD_NAME, MODEL_NAME, MODEL_FIELD_NAME + ModelKeyParser.NULL_BYTE + REVERSE.getValue(), COLVIZ, TIMESTAMP);
        k = ModelKeyParser.createKey(REVERSE_FIELD_MAPPING, MODEL_NAME);
        assertEquals(REVERSE_KEY, k);
    }

    @Test
    public void testForwardCreateMutation() {
        Mutation m = ModelKeyParser.createMutation(FORWARD_FIELD_MAPPING, MODEL_NAME);
        m.getUpdates();
        assertEquals(FORWARD_MUTATION, m);

        // Test with null datatype
        FORWARD_FIELD_MAPPING.setDatatype(null);
        m = ModelKeyParser.createMutation(FORWARD_FIELD_MAPPING, MODEL_NAME);
        FORWARD_MUTATION = new Mutation(MODEL_FIELD_NAME);
        FORWARD_MUTATION.put(MODEL_NAME, FIELD_NAME + ModelKeyParser.NULL_BYTE + FORWARD.getValue(), new ColumnVisibility(COLVIZ), TIMESTAMP,
                        ModelKeyParser.NULL_VALUE);
        m.getUpdates();
        assertEquals(FORWARD_MUTATION, m);
    }

    @Test
    public void testStrictCreateMutation() {
        Mutation m = ModelKeyParser.createMutation(STRICT_MAPPING, MODEL_NAME);
        m.getUpdates();
        assertEquals(STRICT_MUTATION, m);

        // Test with null datatype
        STRICT_MAPPING.setDatatype(null);
        m = ModelKeyParser.createMutation(STRICT_MAPPING, MODEL_NAME);
        STRICT_MUTATION = new Mutation(MODEL_FIELD_NAME);
        STRICT_MUTATION.put(MODEL_NAME, QueryModel.STRICT, new ColumnVisibility(COLVIZ), TIMESTAMP, ModelKeyParser.NULL_VALUE);
        m.getUpdates();
        assertEquals(STRICT_MUTATION, m);
    }

    @Test
    public void testReverseCreateMutation() {
        Mutation m = ModelKeyParser.createMutation(REVERSE_FIELD_MAPPING, MODEL_NAME);
        m.getUpdates();
        assertEquals(REVERSE_MUTATION, m);

        // Test with null datatype
        REVERSE_FIELD_MAPPING.setDatatype(null);
        m = ModelKeyParser.createMutation(REVERSE_FIELD_MAPPING, MODEL_NAME);
        REVERSE_MUTATION = new Mutation(FIELD_NAME);
        REVERSE_MUTATION.put(MODEL_NAME, MODEL_FIELD_NAME + ModelKeyParser.NULL_BYTE + REVERSE.getValue(), new ColumnVisibility(COLVIZ), TIMESTAMP,
                        ModelKeyParser.NULL_VALUE);
        m.getUpdates();
        assertEquals(REVERSE_MUTATION, m);
    }

    @Test
    public void testForwardCreateDeleteMutation() {
        Mutation m = ModelKeyParser.createDeleteMutation(FORWARD_FIELD_MAPPING, MODEL_NAME);
        m.getUpdates();
        assertEquals(FORWARD_DELETE_MUTATION, m);

        // Test with null datatype
        FORWARD_FIELD_MAPPING.setDatatype(null);
        FORWARD_DELETE_MUTATION = new Mutation(MODEL_FIELD_NAME);
        FORWARD_DELETE_MUTATION.putDelete(MODEL_NAME, FIELD_NAME + ModelKeyParser.NULL_BYTE + FORWARD.getValue(), new ColumnVisibility(COLVIZ), TIMESTAMP);
        FORWARD_DELETE_MUTATION.putDelete(MODEL_NAME, FIELD_NAME + ModelKeyParser.NULL_BYTE + "index_only" + ModelKeyParser.NULL_BYTE + FORWARD.getValue(),
                        new ColumnVisibility(COLVIZ), TIMESTAMP);
        m = ModelKeyParser.createDeleteMutation(FORWARD_FIELD_MAPPING, MODEL_NAME);
        m.getUpdates();
        assertEquals(FORWARD_DELETE_MUTATION, m);
    }

    @Test
    public void testStrictCreateDeleteMutation() {
        Mutation m = ModelKeyParser.createDeleteMutation(STRICT_MAPPING, MODEL_NAME);
        m.getUpdates();
        assertEquals(STRICT_DELETE_MUTATION, m);

        // Test with null datatype
        STRICT_MAPPING.setDatatype(null);
        STRICT_DELETE_MUTATION = new Mutation(MODEL_FIELD_NAME);
        STRICT_DELETE_MUTATION.putDelete(MODEL_NAME, ModelKeyParser.ATTRIBUTES, new ColumnVisibility(COLVIZ), TIMESTAMP);
        STRICT_DELETE_MUTATION.putDelete(MODEL_NAME, QueryModel.STRICT, new ColumnVisibility(COLVIZ), TIMESTAMP);
        m = ModelKeyParser.createDeleteMutation(STRICT_MAPPING, MODEL_NAME);
        m.getUpdates();
        assertEquals(STRICT_DELETE_MUTATION, m);
    }

    @Test
    public void testReverseCreateDeleteMutation() {
        Mutation m = ModelKeyParser.createDeleteMutation(REVERSE_FIELD_MAPPING, MODEL_NAME);
        m.getUpdates();
        assertEquals(REVERSE_DELETE_MUTATION, m);

        // Test with null datatype
        REVERSE_FIELD_MAPPING.setDatatype(null);
        REVERSE_DELETE_MUTATION = new Mutation(FIELD_NAME);
        REVERSE_DELETE_MUTATION.putDelete(MODEL_NAME, MODEL_FIELD_NAME + ModelKeyParser.NULL_BYTE + REVERSE.getValue(), new ColumnVisibility(COLVIZ),
                        TIMESTAMP);
        m = ModelKeyParser.createDeleteMutation(REVERSE_FIELD_MAPPING, MODEL_NAME);
        m.getUpdates();
        assertEquals(REVERSE_DELETE_MUTATION, m);
    }

    @Test
    public void testParseKeyNullCV() {
        FieldMapping mapping = ModelKeyParser.parseKey(NULL_CV_KEY);
        assertEquals(NULL_CV_MAPPING, mapping);
    }

    @Test
    public void testForwardMappingIndexOnlyParse() {

        // Test with datatype
        FieldMapping forwardMapping = new FieldMapping();
        forwardMapping.setColumnVisibility(COLVIZ);
        forwardMapping.setDatatype(DATATYPE);
        forwardMapping.setDirection(FORWARD);
        forwardMapping.setFieldName(FIELD_NAME);
        forwardMapping.setModelFieldName(MODEL_FIELD_NAME);

        Key expectedForwardKey = new Key(MODEL_FIELD_NAME, MODEL_NAME + ModelKeyParser.NULL_BYTE + DATATYPE,
                        FIELD_NAME + ModelKeyParser.NULL_BYTE + FORWARD.getValue(), COLVIZ, TIMESTAMP);

        Key k = ModelKeyParser.createKey(forwardMapping, MODEL_NAME);
        assertEquals(expectedForwardKey, k);

        // Test without datatype
        forwardMapping = new FieldMapping();
        forwardMapping.setColumnVisibility(COLVIZ);
        forwardMapping.setDirection(FORWARD);
        forwardMapping.setFieldName(FIELD_NAME);
        forwardMapping.setModelFieldName(MODEL_FIELD_NAME);

        expectedForwardKey = new Key(MODEL_FIELD_NAME, MODEL_NAME, FIELD_NAME + ModelKeyParser.NULL_BYTE + FORWARD.getValue(), COLVIZ, TIMESTAMP);

        k = ModelKeyParser.createKey(forwardMapping, MODEL_NAME);
        assertEquals(expectedForwardKey, k);
    }

    @Test
    public void testForwardIndexOnlyCreateMutation() {
        FieldMapping forwardMapping = new FieldMapping();
        forwardMapping.setColumnVisibility("PRIVATE");
        forwardMapping.setDatatype(DATATYPE);
        forwardMapping.setDirection(FORWARD);
        forwardMapping.setFieldName(FIELD_NAME);
        forwardMapping.setModelFieldName(MODEL_FIELD_NAME);

        Mutation expectedforwardMutation = new Mutation(MODEL_FIELD_NAME);
        Text cf = new Text(MODEL_NAME + ModelKeyParser.NULL_BYTE + DATATYPE);
        Text cq = new Text(FIELD_NAME + ModelKeyParser.NULL_BYTE + FORWARD.getValue());
        expectedforwardMutation.put(cf, cq, new ColumnVisibility(COLVIZ), TIMESTAMP, ModelKeyParser.NULL_VALUE);

        Mutation m = ModelKeyParser.createMutation(forwardMapping, MODEL_NAME);
        m.getUpdates();
        assertTrue(expectedforwardMutation.equals(m), "Expected true: expectedforwardMutation.equals(m)");

        // Without Datatype
        forwardMapping = new FieldMapping();
        forwardMapping.setColumnVisibility(COLVIZ);
        forwardMapping.setDirection(FORWARD);
        forwardMapping.setFieldName(FIELD_NAME);
        forwardMapping.setModelFieldName(MODEL_FIELD_NAME);

        expectedforwardMutation = new Mutation(MODEL_FIELD_NAME);
        cf = new Text(MODEL_NAME);
        cq = new Text(FIELD_NAME + ModelKeyParser.NULL_BYTE + FORWARD.getValue());
        expectedforwardMutation.put(cf, cq, new ColumnVisibility(COLVIZ), TIMESTAMP, ModelKeyParser.NULL_VALUE);

        m = ModelKeyParser.createMutation(forwardMapping, MODEL_NAME);
        m.getUpdates();
        assertTrue(expectedforwardMutation.equals(m), "Expected true: expectedforwardMutation.equals(m)");
    }

    /**
     * Test boundary conditions on ForwardKeyParsing / Trigger failure conditions
     *
     * @throws Exception
     */

    @Test
    public void testKeyWithInvalidDirection() {
        Key keyWrongDirection = new Key(MODEL_FIELD_NAME, MODEL_NAME, FIELD_NAME + ModelKeyParser.NULL_BYTE + "someInvalidDirection", COLVIZ, TIMESTAMP);
        assertThrows(IllegalArgumentException.class, () -> ModelKeyParser.parseKey(keyWrongDirection));
    }

    @Test
    public void testKeyWithTooManyPartsInColQualifier() {
        Key keyTooManyParts = new Key(MODEL_FIELD_NAME, MODEL_NAME, FIELD_NAME + ModelKeyParser.NULL_BYTE + FORWARD.getValue() + ModelKeyParser.NULL_BYTE
                        + "index_only" + ModelKeyParser.NULL_BYTE + REVERSE.getValue(), COLVIZ, TIMESTAMP);
        assertThrows(IllegalArgumentException.class, () -> ModelKeyParser.parseKey(keyTooManyParts));
    }

    @Test
    public void testKeyWithIncorrectlyPositionedIndexOnlyAndDirection() {
        // Correct cq: field\x00
        Key mismatchedParts = new Key(MODEL_FIELD_NAME, MODEL_NAME,
                        FIELD_NAME + ModelKeyParser.NULL_BYTE + FORWARD.getValue() + ModelKeyParser.NULL_BYTE + "index_only", COLVIZ, TIMESTAMP);
        assertThrows(IllegalArgumentException.class, () -> ModelKeyParser.parseKey(mismatchedParts));
    }

    @Test
    public void testIndexOnlyOnAReverseKeyIsInvalid() {
        // Test index_only on a reverse key.. reverse keys should not have index_only
        Key reverseIndexOnly = new Key(MODEL_FIELD_NAME, MODEL_NAME, FIELD_NAME + ModelKeyParser.NULL_BYTE + "index_only" + REVERSE.getValue(), COLVIZ,
                        TIMESTAMP);
        assertThrows(IllegalArgumentException.class, () -> ModelKeyParser.parseKey(reverseIndexOnly));
    }
}
