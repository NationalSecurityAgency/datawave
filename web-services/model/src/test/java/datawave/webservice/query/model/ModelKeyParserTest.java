package datawave.webservice.query.model;

import datawave.webservice.model.Direction;
import datawave.webservice.model.FieldMapping;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.easymock.EasyMock;
import org.easymock.EasyMockExtension;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collections;
import java.util.Set;

@Disabled
@ExtendWith(EasyMockExtension.class)
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
    private static FieldMapping REVERSE_FIELD_MAPPING = null;
    private static FieldMapping NULL_CV_MAPPING = null;
    private static Key FORWARD_KEY = null;
    private static Key REVERSE_KEY = null;
    private static Key NULL_CV_KEY = null;
    private static Mutation FORWARD_MUTATION = null;
    private static Mutation FORWARD_DELETE_MUTATION = null;
    private static Mutation REVERSE_MUTATION = null;
    private static Mutation REVERSE_DELETE_MUTATION = null;
    
    private static long TIMESTAMP = System.currentTimeMillis();
    
    @BeforeEach
    public void setup() throws Exception {
        FORWARD_FIELD_MAPPING = new FieldMapping();
        FORWARD_FIELD_MAPPING.setColumnVisibility(COLVIZ);
        FORWARD_FIELD_MAPPING.setDatatype(DATATYPE);
        FORWARD_FIELD_MAPPING.setDirection(FORWARD);
        FORWARD_FIELD_MAPPING.setFieldName(FIELD_NAME);
        FORWARD_FIELD_MAPPING.setModelFieldName(MODEL_FIELD_NAME);
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
        FORWARD_KEY = new Key(MODEL_FIELD_NAME, MODEL_NAME + ModelKeyParser.NULL_BYTE + DATATYPE, FIELD_NAME + ModelKeyParser.NULL_BYTE + FORWARD.getValue(),
                        COLVIZ, TIMESTAMP);
        REVERSE_KEY = new Key(FIELD_NAME, MODEL_NAME + ModelKeyParser.NULL_BYTE + DATATYPE, MODEL_FIELD_NAME + ModelKeyParser.NULL_BYTE + REVERSE.getValue(),
                        COLVIZ, TIMESTAMP);
        NULL_CV_KEY = new Key(FIELD_NAME, MODEL_NAME + ModelKeyParser.NULL_BYTE + DATATYPE, MODEL_FIELD_NAME + ModelKeyParser.NULL_BYTE + REVERSE.getValue(),
                        "", TIMESTAMP);
        FORWARD_MUTATION = new Mutation(MODEL_FIELD_NAME);
        FORWARD_MUTATION.put(MODEL_NAME + ModelKeyParser.NULL_BYTE + DATATYPE, FIELD_NAME + ModelKeyParser.NULL_BYTE + FORWARD.getValue(),
                        new ColumnVisibility(COLVIZ), TIMESTAMP, ModelKeyParser.NULL_VALUE);
        FORWARD_DELETE_MUTATION = new Mutation(MODEL_FIELD_NAME);
        FORWARD_DELETE_MUTATION.putDelete(MODEL_NAME + ModelKeyParser.NULL_BYTE + DATATYPE, FIELD_NAME + ModelKeyParser.NULL_BYTE + FORWARD.getValue(),
                        new ColumnVisibility(COLVIZ), TIMESTAMP);
        FORWARD_DELETE_MUTATION.putDelete(MODEL_NAME + ModelKeyParser.NULL_BYTE + DATATYPE, FIELD_NAME + ModelKeyParser.NULL_BYTE + "index_only"
                        + ModelKeyParser.NULL_BYTE + FORWARD.getValue(), new ColumnVisibility(COLVIZ), TIMESTAMP);
        
        REVERSE_MUTATION = new Mutation(FIELD_NAME);
        REVERSE_MUTATION.put(MODEL_NAME + ModelKeyParser.NULL_BYTE + DATATYPE, MODEL_FIELD_NAME + ModelKeyParser.NULL_BYTE + REVERSE.getValue(),
                        new ColumnVisibility(COLVIZ), TIMESTAMP, ModelKeyParser.NULL_VALUE);
        REVERSE_DELETE_MUTATION = new Mutation(FIELD_NAME);
        REVERSE_DELETE_MUTATION.putDelete(MODEL_NAME + ModelKeyParser.NULL_BYTE + DATATYPE, MODEL_FIELD_NAME + ModelKeyParser.NULL_BYTE + REVERSE.getValue(),
                        new ColumnVisibility(COLVIZ), TIMESTAMP);
        
        // PowerMock.mockStatic(System.class, System.class.getMethod("currentTimeMillis"));
    }
    
    @Test
    public void testForwardKeyParse() throws Exception {
        FieldMapping mapping = ModelKeyParser.parseKey(FORWARD_KEY, AUTHS);
        Assertions.assertEquals(FORWARD_FIELD_MAPPING, mapping);
        
        // Test ForwardKeyParse with no datatype
        FORWARD_FIELD_MAPPING.setDatatype(null);
        FORWARD_KEY = new Key(MODEL_FIELD_NAME, MODEL_NAME, FIELD_NAME + ModelKeyParser.NULL_BYTE + FORWARD.getValue(), COLVIZ, TIMESTAMP);
        
        mapping = ModelKeyParser.parseKey(FORWARD_KEY, AUTHS);
        Assertions.assertEquals(FORWARD_FIELD_MAPPING, mapping);
    }
    
    @Test
    public void testReverseKeyParse() throws Exception {
        FieldMapping mapping = ModelKeyParser.parseKey(REVERSE_KEY, AUTHS);
        Assertions.assertEquals(REVERSE_FIELD_MAPPING, mapping);
        
        // Test ReverseKeyParse with no datatype
        REVERSE_FIELD_MAPPING.setDatatype(null);
        REVERSE_KEY = new Key(FIELD_NAME, MODEL_NAME, MODEL_FIELD_NAME + ModelKeyParser.NULL_BYTE + REVERSE.getValue(), COLVIZ, TIMESTAMP);
        mapping = ModelKeyParser.parseKey(REVERSE_KEY, AUTHS);
        Assertions.assertEquals(REVERSE_FIELD_MAPPING, mapping, "ReverseKeyParse with no datatype failed.");
    }
    
    @Test
    public void testForwardMappingParse() throws Exception {
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        EasyMock.replay();
        Key k = ModelKeyParser.createKey(FORWARD_FIELD_MAPPING, MODEL_NAME);
        EasyMock.verify();
        Assertions.assertEquals(FORWARD_KEY, k);
        
        // Test forwardMappingParse with null datatype
        EasyMock.reset();
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        FORWARD_FIELD_MAPPING.setDatatype(null);
        EasyMock.replay();
        k = ModelKeyParser.createKey(FORWARD_FIELD_MAPPING, MODEL_NAME);
        EasyMock.verify();
        FORWARD_KEY = new Key(MODEL_FIELD_NAME, MODEL_NAME, FIELD_NAME + ModelKeyParser.NULL_BYTE + FORWARD.getValue(), COLVIZ, TIMESTAMP);
        EasyMock.verify();
        Assertions.assertEquals(FORWARD_KEY, k);
    }
    
    @Test
    public void testReverseMappingParse() throws Exception {
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        EasyMock.replay();
        Key k = ModelKeyParser.createKey(REVERSE_FIELD_MAPPING, MODEL_NAME);
        EasyMock.verify();
        Assertions.assertEquals(REVERSE_KEY, k);
        
        // Test with null datatype
        EasyMock.reset();
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        REVERSE_FIELD_MAPPING.setDatatype(null);
        EasyMock.replay();
        REVERSE_KEY = new Key(FIELD_NAME, MODEL_NAME, MODEL_FIELD_NAME + ModelKeyParser.NULL_BYTE + REVERSE.getValue(), COLVIZ, TIMESTAMP);
        k = ModelKeyParser.createKey(REVERSE_FIELD_MAPPING, MODEL_NAME);
        EasyMock.verify();
        Assertions.assertEquals(REVERSE_KEY, k);
    }
    
    @Test
    public void testForwardCreateMutation() throws Exception {
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        EasyMock.replay();
        Mutation m = ModelKeyParser.createMutation(FORWARD_FIELD_MAPPING, MODEL_NAME);
        EasyMock.verify();
        m.getUpdates();
        Assertions.assertEquals(FORWARD_MUTATION, m);
        
        // Test with null datatype
        EasyMock.reset();
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        FORWARD_FIELD_MAPPING.setDatatype(null);
        EasyMock.replay();
        m = ModelKeyParser.createMutation(FORWARD_FIELD_MAPPING, MODEL_NAME);
        FORWARD_MUTATION = new Mutation(MODEL_FIELD_NAME);
        FORWARD_MUTATION.put(MODEL_NAME, FIELD_NAME + ModelKeyParser.NULL_BYTE + FORWARD.getValue(), new ColumnVisibility(COLVIZ), TIMESTAMP,
                        ModelKeyParser.NULL_VALUE);
        EasyMock.verify();
        m.getUpdates();
        Assertions.assertEquals(FORWARD_MUTATION, m);
    }
    
    @Test
    public void testReverseCreateMutation() throws Exception {
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        EasyMock.replay();
        Mutation m = ModelKeyParser.createMutation(REVERSE_FIELD_MAPPING, MODEL_NAME);
        EasyMock.verify();
        m.getUpdates();
        Assertions.assertEquals(REVERSE_MUTATION, m);
        
        // Test with null datatype
        EasyMock.reset();
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        REVERSE_FIELD_MAPPING.setDatatype(null);
        EasyMock.replay();
        m = ModelKeyParser.createMutation(REVERSE_FIELD_MAPPING, MODEL_NAME);
        REVERSE_MUTATION = new Mutation(FIELD_NAME);
        REVERSE_MUTATION.put(MODEL_NAME, MODEL_FIELD_NAME + ModelKeyParser.NULL_BYTE + REVERSE.getValue(), new ColumnVisibility(COLVIZ), TIMESTAMP,
                        ModelKeyParser.NULL_VALUE);
        EasyMock.verify();
        m.getUpdates();
        Assertions.assertEquals(REVERSE_MUTATION, m);
    }
    
    @Test
    public void testForwardCreateDeleteMutation() throws Exception {
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP).times(2);
        EasyMock.replay();
        Mutation m = ModelKeyParser.createDeleteMutation(FORWARD_FIELD_MAPPING, MODEL_NAME);
        EasyMock.verify();
        m.getUpdates();
        Assertions.assertEquals(FORWARD_DELETE_MUTATION, m);
        
        // Test with null datatype
        EasyMock.reset();
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP).times(2);
        FORWARD_FIELD_MAPPING.setDatatype(null);
        EasyMock.replay();
        FORWARD_DELETE_MUTATION = new Mutation(MODEL_FIELD_NAME);
        FORWARD_DELETE_MUTATION.putDelete(MODEL_NAME, FIELD_NAME + ModelKeyParser.NULL_BYTE + FORWARD.getValue(), new ColumnVisibility(COLVIZ), TIMESTAMP);
        FORWARD_DELETE_MUTATION.putDelete(MODEL_NAME, FIELD_NAME + ModelKeyParser.NULL_BYTE + "index_only" + ModelKeyParser.NULL_BYTE + FORWARD.getValue(),
                        new ColumnVisibility(COLVIZ), TIMESTAMP);
        m = ModelKeyParser.createDeleteMutation(FORWARD_FIELD_MAPPING, MODEL_NAME);
        EasyMock.verify();
        m.getUpdates();
        Assertions.assertEquals(FORWARD_DELETE_MUTATION, m);
    }
    
    @Test
    public void testReverseCreateDeleteMutation() throws Exception {
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        EasyMock.replay();
        Mutation m = ModelKeyParser.createDeleteMutation(REVERSE_FIELD_MAPPING, MODEL_NAME);
        EasyMock.verify();
        m.getUpdates();
        Assertions.assertEquals(REVERSE_DELETE_MUTATION, m);
        
        // Test with null datatype
        EasyMock.reset();
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        REVERSE_FIELD_MAPPING.setDatatype(null);
        EasyMock.replay();
        REVERSE_DELETE_MUTATION = new Mutation(FIELD_NAME);
        REVERSE_DELETE_MUTATION
                        .putDelete(MODEL_NAME, MODEL_FIELD_NAME + ModelKeyParser.NULL_BYTE + REVERSE.getValue(), new ColumnVisibility(COLVIZ), TIMESTAMP);
        m = ModelKeyParser.createDeleteMutation(REVERSE_FIELD_MAPPING, MODEL_NAME);
        EasyMock.verify();
        m.getUpdates();
        Assertions.assertEquals(REVERSE_DELETE_MUTATION, m);
    }
    
    @Test
    public void testParseKeyNullCV() throws Exception {
        FieldMapping mapping = ModelKeyParser.parseKey(NULL_CV_KEY, AUTHS);
        Assertions.assertEquals(NULL_CV_MAPPING, mapping);
    }
    
    @Test
    public void testForwardMappingIndexOnlyParse() throws Exception {
        
        // Test with datatype
        FieldMapping forwardMapping = new FieldMapping();
        forwardMapping.setColumnVisibility(COLVIZ);
        forwardMapping.setDatatype(DATATYPE);
        forwardMapping.setDirection(FORWARD);
        forwardMapping.setFieldName(FIELD_NAME);
        forwardMapping.setModelFieldName(MODEL_FIELD_NAME);
        
        Key expectedForwardKey = new Key(MODEL_FIELD_NAME, MODEL_NAME + ModelKeyParser.NULL_BYTE + DATATYPE, FIELD_NAME + ModelKeyParser.NULL_BYTE
                        + FORWARD.getValue(), COLVIZ, TIMESTAMP);
        
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        EasyMock.replay();
        Key k = ModelKeyParser.createKey(forwardMapping, MODEL_NAME);
        Assertions.assertEquals(expectedForwardKey, k);
        
        // Test without datatype
        EasyMock.reset();
        forwardMapping = new FieldMapping();
        forwardMapping.setColumnVisibility(COLVIZ);
        forwardMapping.setDirection(FORWARD);
        forwardMapping.setFieldName(FIELD_NAME);
        forwardMapping.setModelFieldName(MODEL_FIELD_NAME);
        
        expectedForwardKey = new Key(MODEL_FIELD_NAME, MODEL_NAME, FIELD_NAME + ModelKeyParser.NULL_BYTE + FORWARD.getValue(), COLVIZ, TIMESTAMP);
        
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        EasyMock.replay();
        k = ModelKeyParser.createKey(forwardMapping, MODEL_NAME);
        Assertions.assertEquals(expectedForwardKey, k);
    }
    
    @Test
    public void testForwardIndexOnlyCreateMutation() throws Exception {
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
        
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        EasyMock.replay();
        Mutation m = ModelKeyParser.createMutation(forwardMapping, MODEL_NAME);
        m.getUpdates();
        Assertions.assertTrue(expectedforwardMutation.equals(m), "Expected true: expectedforwardMutation.equals(m)");
        
        // Without Datatype
        EasyMock.reset();
        forwardMapping = new FieldMapping();
        forwardMapping.setColumnVisibility(COLVIZ);
        forwardMapping.setDirection(FORWARD);
        forwardMapping.setFieldName(FIELD_NAME);
        forwardMapping.setModelFieldName(MODEL_FIELD_NAME);
        
        expectedforwardMutation = new Mutation(MODEL_FIELD_NAME);
        cf = new Text(MODEL_NAME);
        cq = new Text(FIELD_NAME + ModelKeyParser.NULL_BYTE + FORWARD.getValue());
        expectedforwardMutation.put(cf, cq, new ColumnVisibility(COLVIZ), TIMESTAMP, ModelKeyParser.NULL_VALUE);
        
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        EasyMock.replay();
        m = ModelKeyParser.createMutation(forwardMapping, MODEL_NAME);
        m.getUpdates();
        Assertions.assertTrue(expectedforwardMutation.equals(m), "Expected true: expectedforwardMutation.equals(m)");
    }
    
    /**
     * Test boundary conditions on ForwardKeyParsing / Trigger failure conditions
     * 
     * @throws Exception
     */
    
    @Test
    public void testKeyWithNoDirection() throws Exception {
        // Test key with no direction
        Key keyNoDirection = new Key(MODEL_FIELD_NAME, MODEL_NAME, FIELD_NAME, COLVIZ, TIMESTAMP);
        Assertions.assertThrows(IllegalArgumentException.class, () -> ModelKeyParser.parseKey(keyNoDirection, AUTHS));
    }
    
    @Test
    public void testKeyWithInvalidDirection() throws Exception {
        Key keyWrongDirection = new Key(MODEL_FIELD_NAME, MODEL_NAME, FIELD_NAME + ModelKeyParser.NULL_BYTE + "someInvalidDirection", COLVIZ, TIMESTAMP);
        Assertions.assertThrows(IllegalArgumentException.class, () -> ModelKeyParser.parseKey(keyWrongDirection, AUTHS));
    }
    
    @Test
    public void testKeyWithTooManyPartsInColQualifier() throws Exception {
        Key keyTooManyParts = new Key(MODEL_FIELD_NAME, MODEL_NAME, FIELD_NAME + ModelKeyParser.NULL_BYTE + FORWARD.getValue() + ModelKeyParser.NULL_BYTE
                        + "index_only" + ModelKeyParser.NULL_BYTE + REVERSE.getValue(), COLVIZ, TIMESTAMP);
        Assertions.assertThrows(IllegalArgumentException.class, () -> ModelKeyParser.parseKey(keyTooManyParts, AUTHS));
    }
    
    @Test
    public void testKeyWithIncorrectlyPositionedIndexOnlyAndDirection() throws Exception {
        // Correct cq: field\x00
        Key mismatchedParts = new Key(MODEL_FIELD_NAME, MODEL_NAME, FIELD_NAME + ModelKeyParser.NULL_BYTE + FORWARD.getValue() + ModelKeyParser.NULL_BYTE
                        + "index_only", COLVIZ, TIMESTAMP);
        Assertions.assertThrows(IllegalArgumentException.class, () -> ModelKeyParser.parseKey(mismatchedParts, AUTHS),
                        "Expected IllegalArgumentException on key with 'index_only' and 'forward' in wrong positions.");
    }
    
    @Test
    public void testIndexOnlyOnAReverseKeyIsInvalid() throws Exception {
        // Test index_only on a reverse key.. reverse keys should not have index_only
        Key reverseIndexOnly = new Key(MODEL_FIELD_NAME, MODEL_NAME, FIELD_NAME + ModelKeyParser.NULL_BYTE + "index_only" + REVERSE.getValue(), COLVIZ,
                        TIMESTAMP);
        Assertions.assertThrows(IllegalArgumentException.class, () -> ModelKeyParser.parseKey(reverseIndexOnly, AUTHS));
    }
}
