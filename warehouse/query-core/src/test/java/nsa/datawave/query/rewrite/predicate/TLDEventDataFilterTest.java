package nsa.datawave.query.rewrite.predicate;

import nsa.datawave.query.rewrite.Constants;
import nsa.datawave.query.util.TypeMetadata;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.Before;
import org.junit.Test;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class TLDEventDataFilterTest extends EasyMockSupport {
    private TLDEventDataFilter filter;
    private ASTJexlScript mockScript;
    private TypeMetadata mockAttributeFactory;
    
    @Before
    public void setup() {
        mockScript = createMock(ASTJexlScript.class);
        mockAttributeFactory = createMock(TypeMetadata.class);
    }
    
    @Test
    public void getCurrentField_standardTest() {
        EasyMock.expect(mockScript.jjtGetNumChildren()).andReturn(0).anyTimes();
        replayAll();
        
        // expected key structure
        Key key = new Key("row", "column", "field" + Constants.NULL_BYTE_STRING + "value");
        filter = new TLDEventDataFilter(mockScript, mockAttributeFactory, false, null, null, -1, -1);
        String field = filter.getCurrentField(key);
        
        assertTrue(field.equals("field"));
        
        verifyAll();
    }
    
    @Test
    public void getCurrentField_groupingTest() {
        EasyMock.expect(mockScript.jjtGetNumChildren()).andReturn(0).anyTimes();
        replayAll();
        
        // expected key structure
        Key key = new Key("row", "column", "field.part_1.part_2.part_3" + Constants.NULL_BYTE_STRING + "value");
        filter = new TLDEventDataFilter(mockScript, mockAttributeFactory, false, null, null, -1, -1);
        String field = filter.getCurrentField(key);
        
        assertTrue(field.equals("field"));
        
        verifyAll();
    }
    
    @Test
    public void keep_emptyMapTest() {
        Map<String,Integer> fieldLimits = Collections.emptyMap();
        
        EasyMock.expect(mockScript.jjtGetNumChildren()).andReturn(0).anyTimes();
        replayAll();
        
        // expected key structure
        Key key = new Key("row", "column", "field1" + Constants.NULL_BYTE_STRING + "value");
        filter = new TLDEventDataFilter(mockScript, mockAttributeFactory, false, null, null, 1, -1, fieldLimits, "LIMIT_FIELD");
        
        assertTrue(filter.keep(key));
        assertTrue(filter.getSeekRange(key, key.followingKey(PartialKey.ROW), false) == null);
        
        verifyAll();
    }
    
    @Test
    public void keep_anyFieldTest() {
        Map<String,Integer> fieldLimits = new HashMap<>(1);
        fieldLimits.put(Constants.ANY_FIELD, 1);
        
        EasyMock.expect(mockScript.jjtGetNumChildren()).andReturn(0).anyTimes();
        replayAll();
        
        // expected key structure
        Key key = new Key("row", "dataType" + Constants.NULL + "123.345.456", "field1" + Constants.NULL_BYTE_STRING + "value");
        filter = new TLDEventDataFilter(mockScript, mockAttributeFactory, false, null, null, 1, -1, fieldLimits, "LIMIT_FIELD");
        
        assertTrue(filter.keep(key));
        // increments counts = 1
        assertTrue(filter.apply(new AbstractMap.SimpleEntry<Key,String>(key, null)));
        assertTrue(filter.getSeekRange(key, key.followingKey(PartialKey.ROW), false) == null);
        // does not increment counts so will still return true
        assertTrue(filter.keep(key));
        // increments counts = 2
        assertFalse(filter.apply(new AbstractMap.SimpleEntry<Key,String>(key, null)));
        Range seekRange = filter.getSeekRange(key, key.followingKey(PartialKey.ROW), false);
        assertTrue(seekRange != null);
        assertTrue(seekRange.getStartKey().getRow().equals(key.getRow()));
        assertTrue(seekRange.getStartKey().getColumnFamily().equals(key.getColumnFamily()));
        assertTrue(seekRange.getStartKey().getColumnQualifier().toString().equals("field1" + "\u0001"));
        assertTrue(seekRange.isStartKeyInclusive() == true);
        
        // now fails
        assertFalse(filter.keep(key));
        
        verifyAll();
    }
    
    @Test
    public void keep_limitFieldTest() {
        Map<String,Integer> fieldLimits = new HashMap<>(1);
        fieldLimits.put("field1", 1);
        
        EasyMock.expect(mockScript.jjtGetNumChildren()).andReturn(0).anyTimes();
        replayAll();
        
        Key key1 = new Key("row", "column", "field1" + Constants.NULL_BYTE_STRING + "value");
        Key key2 = new Key("row", "column", "field2" + Constants.NULL_BYTE_STRING + "value");
        filter = new TLDEventDataFilter(mockScript, mockAttributeFactory, false, null, null, 1, -1, fieldLimits, "LIMIT_FIELD");
        
        assertTrue(filter.keep(key1));
        // increments counts = 1
        assertTrue(filter.apply(new AbstractMap.SimpleEntry<Key,String>(key1, null)));
        assertTrue(filter.transform(key1) == null);
        assertTrue(filter.getSeekRange(key1, key1.followingKey(PartialKey.ROW), false) == null);
        // does not increment counts so will still return true
        assertTrue(filter.keep(key1));
        // increments counts = 2
        assertFalse(filter.apply(new AbstractMap.SimpleEntry<Key,String>(key1, null)));
        Range seekRange = filter.getSeekRange(key1, key1.followingKey(PartialKey.ROW), false);
        assertTrue(seekRange != null);
        assertTrue(seekRange.getStartKey().getRow().equals(key1.getRow()));
        assertTrue(seekRange.getStartKey().getColumnFamily().equals(key1.getColumnFamily()));
        assertTrue(seekRange.getStartKey().getColumnQualifier().toString().equals("field1" + "\u0001"));
        assertTrue(seekRange.isStartKeyInclusive() == true);
        // now fails
        assertFalse(filter.keep(key1));
        
        Key limitKey = filter.transform(key1);
        assertTrue(limitKey != null);
        assertTrue(limitKey.getRow().equals(key1.getRow()));
        assertTrue(limitKey.getColumnFamily().equals(key1.getColumnFamily()));
        assertTrue(limitKey.getColumnQualifier().toString().equals("LIMIT_FIELD" + Constants.NULL + "field1"));
        
        // unlimited field
        assertTrue(filter.keep(key2));
        // increments counts = 1
        assertTrue(filter.apply(new AbstractMap.SimpleEntry<Key,String>(key2, null)));
        assertTrue(filter.transform(key2) == null);
        assertTrue(filter.getSeekRange(key2, key2.followingKey(PartialKey.ROW), false) == null);
        
        assertTrue(filter.keep(key2));
        // increments counts = 2
        assertTrue(filter.apply(new AbstractMap.SimpleEntry<Key,String>(key2, null)));
        assertTrue(filter.getSeekRange(key2, key2.followingKey(PartialKey.ROW), false) == null);
        // still passes
        assertTrue(filter.keep(key2));
        assertTrue(filter.transform(key2) == null);
        
        verifyAll();
    }
    
    @Test
    public void getSeekRange_maxFieldSeekNotEqualToLimit() {
        Map<String,Integer> fieldLimits = new HashMap<>(1);
        fieldLimits.put("field1", 1);
        
        EasyMock.expect(mockScript.jjtGetNumChildren()).andReturn(0).anyTimes();
        replayAll();
        
        Key key1 = new Key("row", "column", "field1" + Constants.NULL_BYTE_STRING + "value");
        Key key2 = new Key("row", "column", "field2" + Constants.NULL_BYTE_STRING + "value");
        filter = new TLDEventDataFilter(mockScript, mockAttributeFactory, false, null, null, 3, -1, fieldLimits, "LIMIT_FIELD");
        
        assertTrue(filter.keep(key1));
        // increments counts = 1
        assertTrue(filter.apply(new AbstractMap.SimpleEntry<Key,String>(key1, null)));
        assertTrue(filter.getSeekRange(key1, key1.followingKey(PartialKey.ROW), false) == null);
        // does not increment counts so will still return true
        assertTrue(filter.keep(key1));
        // increments counts = 2
        assertFalse(filter.apply(new AbstractMap.SimpleEntry<Key,String>(key1, null)));
        assertTrue(filter.getSeekRange(key1, key1.followingKey(PartialKey.ROW), false) == null);
        
        // now fails
        assertFalse(filter.keep(key1));
        
        // see another key on apply to trigger the seek range
        assertFalse(filter.apply(new AbstractMap.SimpleEntry<Key,String>(key1, null)));
        Range seekRange = filter.getSeekRange(key1, key1.followingKey(PartialKey.ROW), false);
        assertTrue(seekRange != null);
        assertTrue(seekRange.getStartKey().getRow().equals(key1.getRow()));
        assertTrue(seekRange.getStartKey().getColumnFamily().equals(key1.getColumnFamily()));
        assertTrue(seekRange.getStartKey().getColumnQualifier().toString().equals("field1" + "\u0001"));
        assertTrue(seekRange.isStartKeyInclusive() == true);
        
        verifyAll();
    }
    
    @Test
    public void getParseInfo_isRootTest() {
        EasyMock.expect(mockScript.jjtGetNumChildren()).andReturn(0).anyTimes();
        replayAll();
        
        // expected key structure
        Key key = new Key("row", "dataype" + Constants.NULL + "123.234.345", "field1" + Constants.NULL_BYTE_STRING + "value");
        filter = new TLDEventDataFilter(mockScript, mockAttributeFactory, false, null, null, -1, -1);
        
        TLDEventDataFilter.ParseInfo info = filter.getParseInfo(key);
        assertTrue(info != null);
        assertTrue(info.getField().equals("field1"));
        assertTrue(info.isRoot());
        
        // first two calls are made without the internal update to the cached parseInfo so are calculated independently
        
        key = new Key("row", "dataype" + Constants.NULL + "123.234.345", "field1" + Constants.NULL_BYTE_STRING + "value");
        info = filter.getParseInfo(key);
        assertTrue(info != null);
        assertTrue(info.getField().equals("field1"));
        assertTrue(info.isRoot());
        
        key = new Key("row", "dataype" + Constants.NULL + "123.234.345.1", "field1" + Constants.NULL_BYTE_STRING + "value");
        info = filter.getParseInfo(key);
        assertTrue(info != null);
        assertTrue(info.getField().equals("field1"));
        // this was wrong assumption based when fixed length UID parse assumptions were being made in the TLDEventDataFilter
        assertTrue(info.isRoot());
        
        key = new Key("row", "dataype" + Constants.NULL + "123.234.345", "field1" + Constants.NULL_BYTE_STRING + "value");
        // use the keep method to set the previous call state
        filter.keep(key);
        info = filter.getParseInfo(key);
        assertTrue(info != null);
        assertTrue(info.getField().equals("field1"));
        assertTrue(info.isRoot());
        
        // now test the child and see that it is not root
        key = new Key("row", "dataype" + Constants.NULL + "123.234.345.1", "field1" + Constants.NULL_BYTE_STRING + "value");
        filter.keep(key);
        info = filter.getParseInfo(key);
        assertTrue(info != null);
        assertTrue(info.getField().equals("field1"));
        assertFalse(info.isRoot());
        
        // a second child
        key = new Key("row", "dataype" + Constants.NULL + "123.234.345.2", "field1" + Constants.NULL_BYTE_STRING + "value");
        filter.keep(key);
        info = filter.getParseInfo(key);
        assertTrue(info != null);
        assertTrue(info.getField().equals("field1"));
        assertFalse(info.isRoot());
        
        // a longer child
        key = new Key("row", "dataype" + Constants.NULL + "123.234.345.23", "field1" + Constants.NULL_BYTE_STRING + "value");
        filter.keep(key);
        info = filter.getParseInfo(key);
        assertTrue(info != null);
        assertTrue(info.getField().equals("field1"));
        assertFalse(info.isRoot());
        
        // jump back to the original
        key = new Key("row", "dataype" + Constants.NULL + "123.234.345", "field1" + Constants.NULL_BYTE_STRING + "value");
        filter.keep(key);
        info = filter.getParseInfo(key);
        assertTrue(info != null);
        assertTrue(info.getField().equals("field1"));
        assertTrue(info.isRoot());
        
        verifyAll();
    }
    
    @Test
    public void setDocumentClearParseInfoTest() {
        EasyMock.expect(mockScript.jjtGetNumChildren()).andReturn(0).anyTimes();
        replayAll();
        
        // expected key structure
        Key key1 = new Key("row", "dataype" + Constants.NULL + "123.234.345", "field1" + Constants.NULL_BYTE_STRING + "value");
        Key key2 = new Key("row", "dataype" + Constants.NULL + "123.234.345.1", "field1" + Constants.NULL_BYTE_STRING + "value");
        Key key3 = new Key("row", "dataype" + Constants.NULL + "123.234.34567", "field1" + Constants.NULL_BYTE_STRING + "value");
        filter = new TLDEventDataFilter(mockScript, mockAttributeFactory, false, null, null, -1, -1);
        
        filter.setDocumentKey(key1);
        // set the lastParseInfo to this key
        filter.keep(key1);
        assertFalse(filter.getParseInfo(key2).isRoot());
        filter.keep(key2);
        // breaking contract calling this on a new document without calling set document, do this to illustrate the potential problem
        assertFalse(filter.getParseInfo(key3).isRoot());
        
        // property follow the contract by setting the context for the document first
        filter.setDocumentKey(key2);
        assertTrue(filter.getParseInfo(key2).isRoot());
        
        verifyAll();
    }
    
}
