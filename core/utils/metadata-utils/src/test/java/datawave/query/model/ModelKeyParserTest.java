package datawave.query.model;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest(ModelKeyParser.class)
@PowerMockIgnore("org.apache.log4j")
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
    
    private static long TIMESTAMP = System.currentTimeMillis();
    
    @Before
    public void setup() throws Exception {
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
        
        PowerMock.mockStatic(System.class, System.class.getMethod("currentTimeMillis"));
    }
    
    @Test
    public void testForwardKeyParse() throws Exception {
        FieldMapping mapping = ModelKeyParser.parseKey(FORWARD_KEY);
        Assert.assertEquals(FORWARD_FIELD_MAPPING, mapping);
        
        // Test ForwardKeyParse with no datatype
        FORWARD_FIELD_MAPPING.setDatatype(null);
        FORWARD_KEY = new Key(MODEL_FIELD_NAME, MODEL_NAME, FIELD_NAME + ModelKeyParser.NULL_BYTE + FORWARD.getValue(), COLVIZ, TIMESTAMP);
        
        mapping = ModelKeyParser.parseKey(FORWARD_KEY);
        Assert.assertEquals(FORWARD_FIELD_MAPPING, mapping);
    }
    
    @Test
    public void testStrictKeyParse() throws Exception {
        FieldMapping mapping = ModelKeyParser.parseKey(STRICT_KEY);
        Assert.assertEquals(STRICT_MAPPING, mapping);
        
        // Test ForwardKeyParse with no datatype
        STRICT_MAPPING.setDatatype(null);
        STRICT_KEY = new Key(MODEL_FIELD_NAME, MODEL_NAME, QueryModel.STRICT, COLVIZ, TIMESTAMP);
        
        mapping = ModelKeyParser.parseKey(STRICT_KEY);
        Assert.assertEquals(STRICT_MAPPING, mapping);
    }
    
    @Test
    public void testVersionKeyParse() throws Exception {
        FieldMapping mapping = ModelKeyParser.parseKey(VERSION_KEY1, VERSION_VALUE1);
        Assert.assertEquals(VERSION_MAPPING, mapping);
        mapping = ModelKeyParser.parseKey(VERSION_KEY2, VERSION_VALUE2);
        Assert.assertEquals(VERSION_MAPPING, mapping);
        mapping = ModelKeyParser.parseKey(VERSION_KEY3, VERSION_VALUE3);
        Assert.assertEquals(VERSION_MAPPING, mapping);
    }
    
    @Test
    public void testReverseKeyParse() throws Exception {
        FieldMapping mapping = ModelKeyParser.parseKey(REVERSE_KEY);
        Assert.assertEquals(REVERSE_FIELD_MAPPING, mapping);
        
        // Test ReverseKeyParse with no datatype
        REVERSE_FIELD_MAPPING.setDatatype(null);
        REVERSE_KEY = new Key(FIELD_NAME, MODEL_NAME, MODEL_FIELD_NAME + ModelKeyParser.NULL_BYTE + REVERSE.getValue(), COLVIZ, TIMESTAMP);
        mapping = ModelKeyParser.parseKey(REVERSE_KEY);
        Assert.assertEquals("ReverseKeyParse with no datatype failed.", REVERSE_FIELD_MAPPING, mapping);
    }
    
    @Test
    public void testForwardMappingParse() throws Exception {
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        PowerMock.replayAll();
        Key k = ModelKeyParser.createKey(FORWARD_FIELD_MAPPING, MODEL_NAME);
        PowerMock.verifyAll();
        Assert.assertEquals(FORWARD_KEY, k);
        
        // Test forwardMappingParse with null datatype
        PowerMock.resetAll();
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        FORWARD_FIELD_MAPPING.setDatatype(null);
        PowerMock.replayAll();
        k = ModelKeyParser.createKey(FORWARD_FIELD_MAPPING, MODEL_NAME);
        PowerMock.verifyAll();
        FORWARD_KEY = new Key(MODEL_FIELD_NAME, MODEL_NAME, FIELD_NAME + ModelKeyParser.NULL_BYTE + FORWARD.getValue(), COLVIZ, TIMESTAMP);
        PowerMock.verifyAll();
        Assert.assertEquals(FORWARD_KEY, k);
    }
    
    @Test
    public void testStrictMappingParse() throws Exception {
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        PowerMock.replayAll();
        Key k = ModelKeyParser.createKey(STRICT_MAPPING, MODEL_NAME);
        PowerMock.verifyAll();
        Assert.assertEquals(STRICT_KEY, k);
        
        // Test StrictMappingParse with null datatype
        PowerMock.resetAll();
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        STRICT_MAPPING.setDatatype(null);
        PowerMock.replayAll();
        k = ModelKeyParser.createKey(STRICT_MAPPING, MODEL_NAME);
        PowerMock.verifyAll();
        STRICT_KEY = new Key(MODEL_FIELD_NAME, MODEL_NAME, QueryModel.STRICT, COLVIZ, TIMESTAMP);
        PowerMock.verifyAll();
        Assert.assertEquals(STRICT_KEY, k);
    }
    
    @Test
    public void testReverseMappingParse() throws Exception {
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        PowerMock.replayAll();
        Key k = ModelKeyParser.createKey(REVERSE_FIELD_MAPPING, MODEL_NAME);
        PowerMock.verifyAll();
        Assert.assertEquals(REVERSE_KEY, k);
        
        // Test with null datatype
        PowerMock.resetAll();
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        REVERSE_FIELD_MAPPING.setDatatype(null);
        PowerMock.replayAll();
        REVERSE_KEY = new Key(FIELD_NAME, MODEL_NAME, MODEL_FIELD_NAME + ModelKeyParser.NULL_BYTE + REVERSE.getValue(), COLVIZ, TIMESTAMP);
        k = ModelKeyParser.createKey(REVERSE_FIELD_MAPPING, MODEL_NAME);
        PowerMock.verifyAll();
        Assert.assertEquals(REVERSE_KEY, k);
    }
    
    @Test
    public void testForwardCreateMutation() throws Exception {
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        PowerMock.replayAll();
        Mutation m = ModelKeyParser.createMutation(FORWARD_FIELD_MAPPING, MODEL_NAME);
        PowerMock.verifyAll();
        m.getUpdates();
        Assert.assertEquals(FORWARD_MUTATION, m);
        
        // Test with null datatype
        PowerMock.resetAll();
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        FORWARD_FIELD_MAPPING.setDatatype(null);
        PowerMock.replayAll();
        m = ModelKeyParser.createMutation(FORWARD_FIELD_MAPPING, MODEL_NAME);
        FORWARD_MUTATION = new Mutation(MODEL_FIELD_NAME);
        FORWARD_MUTATION.put(MODEL_NAME, FIELD_NAME + ModelKeyParser.NULL_BYTE + FORWARD.getValue(), new ColumnVisibility(COLVIZ), TIMESTAMP,
                        ModelKeyParser.NULL_VALUE);
        PowerMock.verifyAll();
        m.getUpdates();
        Assert.assertEquals(FORWARD_MUTATION, m);
    }
    
    @Test
    public void testStrictCreateMutation() throws Exception {
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        PowerMock.replayAll();
        Mutation m = ModelKeyParser.createMutation(STRICT_MAPPING, MODEL_NAME);
        PowerMock.verifyAll();
        m.getUpdates();
        Assert.assertEquals(STRICT_MUTATION, m);
        
        // Test with null datatype
        PowerMock.resetAll();
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        STRICT_MAPPING.setDatatype(null);
        PowerMock.replayAll();
        m = ModelKeyParser.createMutation(STRICT_MAPPING, MODEL_NAME);
        STRICT_MUTATION = new Mutation(MODEL_FIELD_NAME);
        STRICT_MUTATION.put(MODEL_NAME, QueryModel.STRICT, new ColumnVisibility(COLVIZ), TIMESTAMP, ModelKeyParser.NULL_VALUE);
        PowerMock.verifyAll();
        m.getUpdates();
        Assert.assertEquals(STRICT_MUTATION, m);
    }
    
    @Test
    public void testReverseCreateMutation() throws Exception {
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        PowerMock.replayAll();
        Mutation m = ModelKeyParser.createMutation(REVERSE_FIELD_MAPPING, MODEL_NAME);
        PowerMock.verifyAll();
        m.getUpdates();
        Assert.assertEquals(REVERSE_MUTATION, m);
        
        // Test with null datatype
        PowerMock.resetAll();
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        REVERSE_FIELD_MAPPING.setDatatype(null);
        PowerMock.replayAll();
        m = ModelKeyParser.createMutation(REVERSE_FIELD_MAPPING, MODEL_NAME);
        REVERSE_MUTATION = new Mutation(FIELD_NAME);
        REVERSE_MUTATION.put(MODEL_NAME, MODEL_FIELD_NAME + ModelKeyParser.NULL_BYTE + REVERSE.getValue(), new ColumnVisibility(COLVIZ), TIMESTAMP,
                        ModelKeyParser.NULL_VALUE);
        PowerMock.verifyAll();
        m.getUpdates();
        Assert.assertEquals(REVERSE_MUTATION, m);
    }
    
    @Test
    public void testForwardCreateDeleteMutation() throws Exception {
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP).times(2);
        PowerMock.replayAll();
        Mutation m = ModelKeyParser.createDeleteMutation(FORWARD_FIELD_MAPPING, MODEL_NAME);
        PowerMock.verifyAll();
        m.getUpdates();
        Assert.assertEquals(FORWARD_DELETE_MUTATION, m);
        
        // Test with null datatype
        PowerMock.resetAll();
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP).times(2);
        FORWARD_FIELD_MAPPING.setDatatype(null);
        PowerMock.replayAll();
        FORWARD_DELETE_MUTATION = new Mutation(MODEL_FIELD_NAME);
        FORWARD_DELETE_MUTATION.putDelete(MODEL_NAME, FIELD_NAME + ModelKeyParser.NULL_BYTE + FORWARD.getValue(), new ColumnVisibility(COLVIZ), TIMESTAMP);
        FORWARD_DELETE_MUTATION.putDelete(MODEL_NAME, FIELD_NAME + ModelKeyParser.NULL_BYTE + "index_only" + ModelKeyParser.NULL_BYTE + FORWARD.getValue(),
                        new ColumnVisibility(COLVIZ), TIMESTAMP);
        m = ModelKeyParser.createDeleteMutation(FORWARD_FIELD_MAPPING, MODEL_NAME);
        PowerMock.verifyAll();
        m.getUpdates();
        Assert.assertEquals(FORWARD_DELETE_MUTATION, m);
    }
    
    @Test
    public void testStrictCreateDeleteMutation() throws Exception {
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP).times(2);
        PowerMock.replayAll();
        Mutation m = ModelKeyParser.createDeleteMutation(STRICT_MAPPING, MODEL_NAME);
        PowerMock.verifyAll();
        m.getUpdates();
        Assert.assertEquals(STRICT_DELETE_MUTATION, m);
        
        // Test with null datatype
        PowerMock.resetAll();
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP).times(2);
        STRICT_MAPPING.setDatatype(null);
        PowerMock.replayAll();
        STRICT_DELETE_MUTATION = new Mutation(MODEL_FIELD_NAME);
        STRICT_DELETE_MUTATION.putDelete(MODEL_NAME, ModelKeyParser.ATTRIBUTES, new ColumnVisibility(COLVIZ), TIMESTAMP);
        STRICT_DELETE_MUTATION.putDelete(MODEL_NAME, QueryModel.STRICT, new ColumnVisibility(COLVIZ), TIMESTAMP);
        m = ModelKeyParser.createDeleteMutation(STRICT_MAPPING, MODEL_NAME);
        PowerMock.verifyAll();
        m.getUpdates();
        Assert.assertEquals(STRICT_DELETE_MUTATION, m);
    }
    
    @Test
    public void testReverseCreateDeleteMutation() throws Exception {
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        PowerMock.replayAll();
        Mutation m = ModelKeyParser.createDeleteMutation(REVERSE_FIELD_MAPPING, MODEL_NAME);
        PowerMock.verifyAll();
        m.getUpdates();
        Assert.assertEquals(REVERSE_DELETE_MUTATION, m);
        
        // Test with null datatype
        PowerMock.resetAll();
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        REVERSE_FIELD_MAPPING.setDatatype(null);
        PowerMock.replayAll();
        REVERSE_DELETE_MUTATION = new Mutation(FIELD_NAME);
        REVERSE_DELETE_MUTATION.putDelete(MODEL_NAME, MODEL_FIELD_NAME + ModelKeyParser.NULL_BYTE + REVERSE.getValue(), new ColumnVisibility(COLVIZ),
                        TIMESTAMP);
        m = ModelKeyParser.createDeleteMutation(REVERSE_FIELD_MAPPING, MODEL_NAME);
        PowerMock.verifyAll();
        m.getUpdates();
        Assert.assertEquals(REVERSE_DELETE_MUTATION, m);
    }
    
    @Test
    public void testParseKeyNullCV() throws Exception {
        FieldMapping mapping = ModelKeyParser.parseKey(NULL_CV_KEY);
        Assert.assertEquals(NULL_CV_MAPPING, mapping);
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
        
        Key expectedForwardKey = new Key(MODEL_FIELD_NAME, MODEL_NAME + ModelKeyParser.NULL_BYTE + DATATYPE,
                        FIELD_NAME + ModelKeyParser.NULL_BYTE + FORWARD.getValue(), COLVIZ, TIMESTAMP);
        
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        PowerMock.replayAll();
        Key k = ModelKeyParser.createKey(forwardMapping, MODEL_NAME);
        Assert.assertEquals(expectedForwardKey, k);
        
        // Test without datatype
        PowerMock.resetAll();
        forwardMapping = new FieldMapping();
        forwardMapping.setColumnVisibility(COLVIZ);
        forwardMapping.setDirection(FORWARD);
        forwardMapping.setFieldName(FIELD_NAME);
        forwardMapping.setModelFieldName(MODEL_FIELD_NAME);
        
        expectedForwardKey = new Key(MODEL_FIELD_NAME, MODEL_NAME, FIELD_NAME + ModelKeyParser.NULL_BYTE + FORWARD.getValue(), COLVIZ, TIMESTAMP);
        
        EasyMock.expect(System.currentTimeMillis()).andReturn(TIMESTAMP);
        PowerMock.replayAll();
        k = ModelKeyParser.createKey(forwardMapping, MODEL_NAME);
        Assert.assertEquals(expectedForwardKey, k);
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
        PowerMock.replayAll();
        Mutation m = ModelKeyParser.createMutation(forwardMapping, MODEL_NAME);
        m.getUpdates();
        Assert.assertTrue("Expected true: expectedforwardMutation.equals(m)", expectedforwardMutation.equals(m));
        
        // Without Datatype
        PowerMock.resetAll();
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
        PowerMock.replayAll();
        m = ModelKeyParser.createMutation(forwardMapping, MODEL_NAME);
        m.getUpdates();
        Assert.assertTrue("Expected true: expectedforwardMutation.equals(m)", expectedforwardMutation.equals(m));
    }
    
    /**
     * Test boundary conditions on ForwardKeyParsing / Trigger failure conditions
     * 
     * @throws Exception
     */
    
    @Test(expected = IllegalArgumentException.class)
    public void testKeyWithInvalidDirection() throws Exception {
        Key keyWrongDirection = new Key(MODEL_FIELD_NAME, MODEL_NAME, FIELD_NAME + ModelKeyParser.NULL_BYTE + "someInvalidDirection", COLVIZ, TIMESTAMP);
        ModelKeyParser.parseKey(keyWrongDirection);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testKeyWithTooManyPartsInColQualifier() throws Exception {
        Key keyTooManyParts = new Key(MODEL_FIELD_NAME, MODEL_NAME, FIELD_NAME + ModelKeyParser.NULL_BYTE + FORWARD.getValue() + ModelKeyParser.NULL_BYTE
                        + "index_only" + ModelKeyParser.NULL_BYTE + REVERSE.getValue(), COLVIZ, TIMESTAMP);
        ModelKeyParser.parseKey(keyTooManyParts);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testKeyWithIncorrectlyPositionedIndexOnlyAndDirection() throws Exception {
        // Correct cq: field\x00
        Key mismatchedParts = new Key(MODEL_FIELD_NAME, MODEL_NAME,
                        FIELD_NAME + ModelKeyParser.NULL_BYTE + FORWARD.getValue() + ModelKeyParser.NULL_BYTE + "index_only", COLVIZ, TIMESTAMP);
        ModelKeyParser.parseKey(mismatchedParts);
        Assert.fail("Expected IllegalArgumentException on key with 'index_only' and 'forward' in wrong positions.");
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testIndexOnlyOnAReverseKeyIsInvalid() throws Exception {
        // Test index_only on a reverse key.. reverse keys should not have index_only
        Key reverseIndexOnly = new Key(MODEL_FIELD_NAME, MODEL_NAME, FIELD_NAME + ModelKeyParser.NULL_BYTE + "index_only" + REVERSE.getValue(), COLVIZ,
                        TIMESTAMP);
        ModelKeyParser.parseKey(reverseIndexOnly);
    }
}
