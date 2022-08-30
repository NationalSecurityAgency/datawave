package datawave.query.transformer;

import datawave.marking.MarkingFunctions;
import datawave.query.Constants;
import datawave.query.DocumentSerialization;
import datawave.query.attributes.Document;
import datawave.query.attributes.Numeric;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.logic.BaseQueryLogic;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.query.result.event.SimpleEvent;
import datawave.webservice.query.result.event.SimpleField;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class DocumentTransformerTest {
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
    
    @Test
    public void transform_noPrimaryToSecondaryMapSetTest() throws MarkingFunctions.Exception {
        Key key = new Key("shard", "dataType" + Constants.NULL + "uid");
        Value value = new Value();
        AbstractMap.SimpleEntry<Key,Value> entry = new AbstractMap.SimpleEntry<>(key, value);
        Document d = new Document();
        
        d.put("field1", new Numeric("5", key, true));
        AbstractMap.SimpleEntry<Key,Document> documentEntry = new AbstractMap.SimpleEntry<>(key, d);
        SimpleField simpleField = new SimpleField();
        SimpleEvent simpleEvent = new SimpleEvent();
        
        // the static mock isn't available outside this block, so any when/assertion statements that stem from
        // the mock must be inside it.
        try (MockedStatic<DocumentSerialization> ds = Mockito.mockStatic(DocumentSerialization.class)) {
            ds.when(() -> DocumentSerialization.getDocumentDeserializer(mockQuery)).thenReturn(mockDeserializer);
            when(mockLogic.getTableName()).thenReturn("table1");
            when(mockQuery.getQueryAuthorizations()).thenReturn("A,B,C");
            when(mockQuery.findParameter("log.timing.details")).thenReturn(new QueryImpl.Parameter("", ""));
            when(mockDeserializer.apply(entry)).thenReturn(documentEntry);
            when(mockMarkingFunctions.translateFromColumnVisibility(key.getColumnVisibilityParsed())).thenReturn(Collections.emptyMap());
            when(mockResponseFactory.getField()).thenReturn(simpleField);
            when(mockResponseFactory.getEvent()).thenReturn(simpleEvent);
            
            transformer = new DocumentTransformer(mockLogic, mockQuery, mockMarkingFunctions, mockResponseFactory, true);
            SimpleEvent event = (SimpleEvent) transformer.transform(entry);
            
            Assertions.assertNotNull(event);
            Assertions.assertEquals(1, event.getFields().size());
            Assertions.assertEquals("field1", event.getFields().get(0).getName());
            Assertions.assertEquals("5", event.getFields().get(0).getValueString());
        }
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
        d.put("field1", new Numeric("5", key, true));
        AbstractMap.SimpleEntry<Key,Document> documentEntry = new AbstractMap.SimpleEntry<>(key, d);
        SimpleField simpleField = new SimpleField();
        SimpleEvent simpleEvent = new SimpleEvent();
        
        // the static mock isn't available outside this block, so any when/assertion statements that stem from
        // the mock must be inside it.
        try (MockedStatic<DocumentSerialization> ds = Mockito.mockStatic(DocumentSerialization.class)) {
            ds.when(() -> DocumentSerialization.getDocumentDeserializer(mockQuery)).thenReturn(mockDeserializer);
            when(mockLogic.getTableName()).thenReturn("table1");
            when(mockQuery.getQueryAuthorizations()).thenReturn("A,B,C");
            when(mockQuery.findParameter("log.timing.details")).thenReturn(new QueryImpl.Parameter("", ""));
            when(mockDeserializer.apply(entry)).thenReturn(documentEntry);
            when(mockMarkingFunctions.translateFromColumnVisibility(key.getColumnVisibilityParsed())).thenReturn(Collections.emptyMap());
            when(mockResponseFactory.getField()).thenReturn(simpleField);
            when(mockResponseFactory.getEvent()).thenReturn(simpleEvent);
            
            transformer = new DocumentTransformer(mockLogic, mockQuery, mockMarkingFunctions, mockResponseFactory, true);
            transformer.setPrimaryToSecondaryFieldMap(fieldMap);
            SimpleEvent event = (SimpleEvent) transformer.transform(entry);
            
            Assertions.assertNotNull(event);
            Assertions.assertEquals(1, event.getFields().size());
            Assertions.assertEquals("field1", event.getFields().get(0).getName());
            Assertions.assertEquals("5", event.getFields().get(0).getValueString());
        }
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
        d.put("field1", new Numeric("5", key, true));
        AbstractMap.SimpleEntry<Key,Document> documentEntry = new AbstractMap.SimpleEntry<>(key, d);
        SimpleField simpleField = new SimpleField();
        SimpleEvent simpleEvent = new SimpleEvent();
        
        // the static mock isn't available outside this block, so any when/assertion statements that stem from
        // the mock must be inside it.
        try (MockedStatic<DocumentSerialization> ds = Mockito.mockStatic(DocumentSerialization.class)) {
            ds.when(() -> DocumentSerialization.getDocumentDeserializer(mockQuery)).thenReturn(mockDeserializer);
            when(mockLogic.getTableName()).thenReturn("table1");
            when(mockQuery.getQueryAuthorizations()).thenReturn("A,B,C");
            when(mockQuery.findParameter("log.timing.details")).thenReturn(new QueryImpl.Parameter("", ""));
            when(mockDeserializer.apply(entry)).thenReturn(documentEntry);
            when(mockMarkingFunctions.translateFromColumnVisibility(key.getColumnVisibilityParsed())).thenReturn(Collections.emptyMap());
            when(mockResponseFactory.getField()).thenReturn(simpleField);
            when(mockResponseFactory.getEvent()).thenReturn(simpleEvent);
            
            transformer = new DocumentTransformer(mockLogic, mockQuery, mockMarkingFunctions, mockResponseFactory, true);
            transformer.setPrimaryToSecondaryFieldMap(fieldMap);
            SimpleEvent event = (SimpleEvent) transformer.transform(entry);
            
            Assertions.assertNotNull(event);
            Assertions.assertEquals(1, event.getFields().size());
            Assertions.assertEquals("field1", event.getFields().get(0).getName());
            Assertions.assertEquals("5", event.getFields().get(0).getValueString());
        }
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
        d.put("field1", new Numeric("5", key, true));
        AbstractMap.SimpleEntry<Key,Document> documentEntry = new AbstractMap.SimpleEntry<>(key, d);
        SimpleField simpleField = new SimpleField();
        SimpleEvent simpleEvent = new SimpleEvent();
        
        // the static mock isn't available outside this block, so any when/assertion statements that stem from
        // the mock must be inside it.
        try (MockedStatic<DocumentSerialization> ds = Mockito.mockStatic(DocumentSerialization.class)) {
            ds.when(() -> DocumentSerialization.getDocumentDeserializer(mockQuery)).thenReturn(mockDeserializer);
            when(mockLogic.getTableName()).thenReturn("table1");
            when(mockQuery.getQueryAuthorizations()).thenReturn("A,B,C");
            when(mockQuery.findParameter("log.timing.details")).thenReturn(new QueryImpl.Parameter("", ""));
            when(mockDeserializer.apply(entry)).thenReturn(documentEntry);
            when(mockMarkingFunctions.translateFromColumnVisibility(key.getColumnVisibilityParsed())).thenReturn(Collections.emptyMap());
            when(mockResponseFactory.getField()).thenReturn(simpleField, new SimpleField());
            when(mockResponseFactory.getEvent()).thenReturn(simpleEvent);
            
            transformer = new DocumentTransformer(mockLogic, mockQuery, mockMarkingFunctions, mockResponseFactory, true);
            transformer.setPrimaryToSecondaryFieldMap(fieldMap);
            SimpleEvent event = (SimpleEvent) transformer.transform(entry);
            
            Assertions.assertNotNull(event);
            Assertions.assertEquals(2, event.getFields().size());
            
            List<String> foundFields = new ArrayList<>(2);
            for (SimpleField field : event.getFields()) {
                foundFields.add(field.getName());
                Assertions.assertEquals("5", field.getValueString());
            }
            
            List<String> expectedFields = new ArrayList<>();
            expectedFields.add("field1");
            expectedFields.add("field2");
            
            Assertions.assertTrue(foundFields.containsAll(expectedFields));
            Assertions.assertTrue(expectedFields.containsAll(foundFields));
        }
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
        d.put("field1", new Numeric("5", key, true));
        AbstractMap.SimpleEntry<Key,Document> documentEntry = new AbstractMap.SimpleEntry<>(key, d);
        SimpleField simpleField = new SimpleField();
        SimpleEvent simpleEvent = new SimpleEvent();
        
        // the static mock isn't available outside this block, so any when/assertion statements that stem from
        // the mock must be inside it.
        try (MockedStatic<DocumentSerialization> ds = Mockito.mockStatic(DocumentSerialization.class)) {
            ds.when(() -> DocumentSerialization.getDocumentDeserializer(mockQuery)).thenReturn(mockDeserializer);
            when(mockLogic.getTableName()).thenReturn("table1");
            when(mockQuery.getQueryAuthorizations()).thenReturn("A,B,C");
            when(mockQuery.findParameter("log.timing.details")).thenReturn(new QueryImpl.Parameter("", ""));
            when(mockDeserializer.apply(entry)).thenReturn(documentEntry);
            when(mockMarkingFunctions.translateFromColumnVisibility(key.getColumnVisibilityParsed())).thenReturn(Collections.emptyMap());
            when(mockResponseFactory.getField()).thenReturn(simpleField, new SimpleField(), new SimpleField());
            when(mockResponseFactory.getEvent()).thenReturn(simpleEvent);
            
            transformer = new DocumentTransformer(mockLogic, mockQuery, mockMarkingFunctions, mockResponseFactory, true);
            transformer.setPrimaryToSecondaryFieldMap(fieldMap);
            SimpleEvent event = (SimpleEvent) transformer.transform(entry);
            
            Assertions.assertNotNull(event);
            Assertions.assertEquals(3, event.getFields().size());
            
            List<String> foundFields = new ArrayList<>(3);
            for (SimpleField field : event.getFields()) {
                foundFields.add(field.getName());
                if (field.getName().equals("field1")) {
                    Assertions.assertEquals("5", field.getValueString());
                } else {
                    Assertions.assertEquals("6", field.getValueString());
                }
            }
            
            List<String> expectedFields = new ArrayList<>();
            expectedFields.add("field1");
            expectedFields.add("field2");
            expectedFields.add("field3");
            
            Assertions.assertTrue(foundFields.containsAll(expectedFields));
            Assertions.assertTrue(expectedFields.containsAll(foundFields));
        }
    }
}
