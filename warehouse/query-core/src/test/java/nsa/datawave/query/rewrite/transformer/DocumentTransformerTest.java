package nsa.datawave.query.rewrite.transformer;

import nsa.datawave.marking.MarkingFunctions;
import nsa.datawave.query.rewrite.Constants;
import nsa.datawave.query.rewrite.DocumentSerialization;
import nsa.datawave.query.rewrite.attributes.Attribute;
import nsa.datawave.query.rewrite.attributes.Attributes;
import nsa.datawave.query.rewrite.attributes.Document;
import nsa.datawave.query.rewrite.attributes.Numeric;
import nsa.datawave.query.rewrite.function.deserializer.DocumentDeserializer;
import nsa.datawave.query.rewrite.function.deserializer.KryoDocumentDeserializer;
import nsa.datawave.webservice.query.Query;
import nsa.datawave.webservice.query.QueryImpl;
import nsa.datawave.webservice.query.logic.BaseQueryLogic;
import nsa.datawave.webservice.query.result.event.ResponseObjectFactory;
import nsa.datawave.webservice.query.result.event.SimpleEvent;
import nsa.datawave.webservice.query.result.event.SimpleField;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(PowerMockRunner.class)
@PrepareForTest({DocumentTransformer.class, DocumentSerialization.class})
public class DocumentTransformerTest { // extends EasyMockSupport {
    private DocumentTransformer transformer;
    
    @Mock
    private BaseQueryLogic mockLogic;
    
    @Mock
    private Query mockQuery;
    
    @Mock
    private MarkingFunctions mockMarkingFunctions;
    
    @Mock
    private ResponseObjectFactory mockResponseFactory;
    
    @Mock
    private KryoDocumentDeserializer mockDeserializer;
    
    // @Mock
    // private Numeric mockNumeric;
    
    // @Mock
    // private Document mockDocument;
    
    @Before
    public void setup() {
        
    }
    
    private void basicExpects(Document d, Key key, Map.Entry<Key,Value> entry) throws MarkingFunctions.Exception {
        // AbstractMap.SimpleEntry<Key, Document> documentEntry = new AbstractMap.SimpleEntry<>(key, mockDocument);
        d.put("field1", new Numeric("5", key, true));
        AbstractMap.SimpleEntry<Key,Document> documentEntry = new AbstractMap.SimpleEntry<>(key, d);
        
        // Map<String,Attribute<?extends Comparable<?>>> dictionary = new HashMap<>();
        // dictionary.put("field1", mockNumeric);
        
        SimpleField simpleField = new SimpleField();
        SimpleEvent simpleEvent = new SimpleEvent();
        
        PowerMock.mockStatic(DocumentSerialization.class);
        EasyMock.expect(DocumentSerialization.getDocumentDeserializer(mockQuery)).andReturn(mockDeserializer);
        
        EasyMock.expect(mockLogic.getTableName()).andReturn("table1");
        EasyMock.expect(mockQuery.getQueryAuthorizations()).andReturn("A,B,C");
        EasyMock.expect(mockQuery.findParameter("log.timing.details")).andReturn(new QueryImpl.Parameter("", ""));
        EasyMock.expect(mockDeserializer.apply(entry)).andReturn(documentEntry);
        // EasyMock.expect(mockDocument.getDictionary()).andReturn(Collections.EMPTY_MAP);
        // mockDocument.debugDocumentSize(key);
        EasyMock.expect(mockMarkingFunctions.translateFromColumnVisibility(key.getColumnVisibilityParsed())).andReturn(Collections.EMPTY_MAP);
        // EasyMock.expect(mockDocument.getDictionary()).andReturn(dictionary);
        // EasyMock.expect(mockNumeric.getData()).andReturn("5");
        EasyMock.expect(mockResponseFactory.getField()).andReturn(simpleField);
        EasyMock.expect(mockResponseFactory.getEvent()).andReturn(simpleEvent);
        // EasyMock.expect(mockDocument.sizeInBytes()).andReturn(1l);
    }
    
    @Test
    public void transform_noPrimaryToSecondaryMapSetTest() throws MarkingFunctions.Exception {
        Key key = new Key("shard", "dataType" + Constants.NULL + "uid");
        Value value = new Value();
        AbstractMap.SimpleEntry<Key,Value> entry = new AbstractMap.SimpleEntry<>(key, value);
        Document d = new Document();
        basicExpects(d, key, entry);
        
        PowerMock.replayAll();
        
        transformer = new DocumentTransformer(mockLogic, mockQuery, mockMarkingFunctions, mockResponseFactory, true);
        SimpleEvent event = (SimpleEvent) transformer.transform(entry);
        
        PowerMock.verifyAll();
        
        Assert.assertNotNull(event);
        Assert.assertTrue(event.getFields().size() == 1);
        Assert.assertTrue(event.getFields().get(0).getName().equals("field1"));
        Assert.assertTrue(event.getFields().get(0).getValueString().equals("5"));
    }
    
    @Test
    public void transform_primaryEmptySecondarySetTest() throws MarkingFunctions.Exception {
        Key key = new Key("shard", "dataType" + Constants.NULL + "uid");
        Value value = new Value();
        AbstractMap.SimpleEntry<Key,Value> entry = new AbstractMap.SimpleEntry<>(key, value);
        
        Map<String,List<String>> fieldMap = new HashMap<>();
        List<String> fieldList = Collections.EMPTY_LIST;
        fieldMap.put("field2", fieldList);
        Document d = new Document();
        basicExpects(d, key, entry);
        
        PowerMock.replayAll();
        
        transformer = new DocumentTransformer(mockLogic, mockQuery, mockMarkingFunctions, mockResponseFactory, true);
        transformer.setPrimaryToSecondaryFieldMap(fieldMap);
        SimpleEvent event = (SimpleEvent) transformer.transform(entry);
        
        PowerMock.verifyAll();
        
        Assert.assertNotNull(event);
        Assert.assertTrue(event.getFields().size() == 1);
        Assert.assertTrue(event.getFields().get(0).getName().equals("field1"));
        Assert.assertTrue(event.getFields().get(0).getValueString().equals("5"));
    }
    
    @Test
    public void transform_primaryNoMatchSetTest() throws MarkingFunctions.Exception {
        Key key = new Key("shard", "dataType" + Constants.NULL + "uid");
        Value value = new Value();
        AbstractMap.SimpleEntry<Key,Value> entry = new AbstractMap.SimpleEntry<>(key, value);
        
        Map<String,List<String>> fieldMap = new HashMap<>();
        List<String> fieldList = new ArrayList<>();
        fieldList.add("field3");
        fieldMap.put("field2", fieldList);
        
        Document d = new Document();
        basicExpects(d, key, entry);
        
        PowerMock.replayAll();
        
        transformer = new DocumentTransformer(mockLogic, mockQuery, mockMarkingFunctions, mockResponseFactory, true);
        transformer.setPrimaryToSecondaryFieldMap(fieldMap);
        SimpleEvent event = (SimpleEvent) transformer.transform(entry);
        
        PowerMock.verifyAll();
        
        Assert.assertNotNull(event);
        Assert.assertTrue(event.getFields().size() == 1);
        Assert.assertTrue(event.getFields().get(0).getName().equals("field1"));
        Assert.assertTrue(event.getFields().get(0).getValueString().equals("5"));
    }
    
    @Test
    public void transform_primaryMatchSetTest() throws MarkingFunctions.Exception {
        Key key = new Key("shard", "dataType" + Constants.NULL + "uid");
        Value value = new Value();
        AbstractMap.SimpleEntry<Key,Value> entry = new AbstractMap.SimpleEntry<>(key, value);
        
        Map<String,List<String>> fieldMap = new HashMap<>();
        List<String> fieldList = new ArrayList<>();
        fieldList.add("field1");
        fieldMap.put("field2", fieldList);
        
        Document d = new Document();
        basicExpects(d, key, entry);
        
        EasyMock.expect(mockResponseFactory.getField()).andReturn(new SimpleField());
        
        PowerMock.replayAll();
        
        transformer = new DocumentTransformer(mockLogic, mockQuery, mockMarkingFunctions, mockResponseFactory, true);
        transformer.setPrimaryToSecondaryFieldMap(fieldMap);
        SimpleEvent event = (SimpleEvent) transformer.transform(entry);
        
        PowerMock.verifyAll();
        
        Assert.assertNotNull(event);
        Assert.assertTrue(event.getFields().size() == 2);
        
        List<String> foundFields = new ArrayList<>(2);
        for (SimpleField field : event.getFields()) {
            foundFields.add(field.getName());
            Assert.assertTrue(field.getValueString().equals("5"));
        }
        
        List<String> expectedFields = new ArrayList<>();
        expectedFields.add("field1");
        expectedFields.add("field2");
        
        Assert.assertTrue(foundFields.containsAll(expectedFields));
        Assert.assertTrue(expectedFields.containsAll(foundFields));
    }
    
    @Test
    public void transform_primaryMatchOrderSetTest() throws MarkingFunctions.Exception {
        Key key = new Key("shard", "dataType" + Constants.NULL + "uid");
        Value value = new Value();
        AbstractMap.SimpleEntry<Key,Value> entry = new AbstractMap.SimpleEntry<>(key, value);
        
        Map<String,List<String>> fieldMap = new HashMap<>();
        List<String> fieldList = new ArrayList<>();
        fieldList.add("field3");
        fieldList.add("field1");
        
        fieldMap.put("field2", fieldList);
        
        Document d = new Document();
        d.put("field3", new Numeric("6", key, true));
        basicExpects(d, key, entry);
        
        EasyMock.expect(mockResponseFactory.getField()).andReturn(new SimpleField());
        EasyMock.expect(mockResponseFactory.getField()).andReturn(new SimpleField());
        
        PowerMock.replayAll();
        
        transformer = new DocumentTransformer(mockLogic, mockQuery, mockMarkingFunctions, mockResponseFactory, true);
        transformer.setPrimaryToSecondaryFieldMap(fieldMap);
        SimpleEvent event = (SimpleEvent) transformer.transform(entry);
        
        PowerMock.verifyAll();
        
        Assert.assertNotNull(event);
        Assert.assertTrue(event.getFields().size() == 3);
        
        List<String> foundFields = new ArrayList<>(3);
        for (SimpleField field : event.getFields()) {
            foundFields.add(field.getName());
            if (field.getName().equals("field1")) {
                Assert.assertTrue(field.getValueString().equals("5"));
            } else {
                Assert.assertTrue(field.getValueString().equals("6"));
            }
        }
        
        List<String> expectedFields = new ArrayList<>();
        expectedFields.add("field1");
        expectedFields.add("field2");
        expectedFields.add("field3");
        
        Assert.assertTrue(foundFields.containsAll(expectedFields));
        Assert.assertTrue(expectedFields.containsAll(foundFields));
    }
}
