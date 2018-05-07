package nsa.datawave.query.rewrite.predicate;

import nsa.datawave.query.rewrite.Constants;
import nsa.datawave.query.util.TypeMetadata;
import org.apache.accumulo.core.data.Key;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.Before;
import org.junit.Test;

import java.util.AbstractMap;

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
        // this a wrong assumption based on DATAWAVE-30 (https://github.com/NationalSecurityAgency/datawave/issues/30)
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
