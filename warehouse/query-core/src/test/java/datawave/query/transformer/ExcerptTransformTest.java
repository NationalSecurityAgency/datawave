package datawave.query.transformer;

import datawave.query.Constants;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Content;
import datawave.query.attributes.Document;
import datawave.query.attributes.ExcerptFields;
import datawave.query.iterator.logic.TermFrequencyExcerptIterator;
import datawave.query.postprocessing.tf.PhraseIndexes;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;
import org.easymock.Capture;
import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;

import static org.easymock.EasyMock.and;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

@RunWith(EasyMockRunner.class)
public class ExcerptTransformTest extends EasyMockSupport {
    
    @Mock
    private TermFrequencyExcerptIterator iterator;
    
    @Mock
    private IteratorEnvironment env;
    
    @Mock
    private SortedKeyValueIterator<Key,Value> source;
    
    private PhraseIndexes phraseIndexes;
    private ExcerptFields excerptFields;
    
    private Document document;
    private ExcerptTransform excerptTransform;
    
    @Before
    public void setUp() throws Exception {
        phraseIndexes = new PhraseIndexes();
        excerptFields = new ExcerptFields();
    }
    
    @After
    public void tearDown() throws Exception {
        document = null;
        excerptTransform = null;
    }
    
    /**
     * Verify that a null entry is returned for a null input.
     */
    @Test
    public void testNullDocumentEntry() {
        initTransform();
        
        replayAll();
        
        assertNull(excerptTransform.apply(null));
        verifyAll();
    }
    
    /**
     * Verify that excerpts are not added for documents that are not marked as to-keep.
     */
    @Test
    public void testNonToKeepDocumentEntry() {
        givenDocument(new Document(new Key(), false));
        initTransform();
        replayAll();
        
        applyTransform();
        
        assertFalse(document.containsKey(ExcerptTransform.HIT_EXCERPTS));
        verifyAll();
    }
    
    /**
     * Verify that excerpts are not added for documents that don't have PHRASE_INDEXES_ATTRIBUTE.
     */
    @Test
    public void testNullPhraseIndexes() {
        givenDocument(new Document(new Key(), true));
        initTransform();
        replayAll();
        
        applyTransform();
        
        assertFalse(document.containsKey(ExcerptTransform.HIT_EXCERPTS));
        verifyAll();
    }
    
    @Test
    public void testExcerpts() throws IOException {
        givenExcerptField("BODY", 2);
        givenPhraseIndex("BODY", 10, 14);
        
        givenMockDocument();
        givenMatchingPhrase("BODY", 8, 16, "the quick brown fox jumped over the lazy dog");
        
        Capture<Attributes> capturedArg = Capture.newInstance();
        document.put(eq(ExcerptTransform.HIT_EXCERPTS), and(capture(capturedArg), isA(Attributes.class)));
        
        initTransform();
        replayAll();
        
        applyTransform();
        verifyAll();
        
        Attributes arg = capturedArg.getValue();
        assertEquals(1, arg.size());
        Attribute<?>[] attributes = arg.getAttributes().toArray(new Attribute[0]);
        assertEquals("the quick brown fox jumped over the lazy dog", ((Content) attributes[0]).getContent());
    }
    
    private void initTransform() {
        excerptTransform = new ExcerptTransform(excerptFields, env, source, iterator);
    }
    
    private void applyTransform() {
        excerptTransform.apply(getDocumentEntry());
    }
    
    private void givenMockDocument() {
        Document document = mock(Document.class);
        
        expect(document.isToKeep()).andReturn(true);
        expect(document.containsKey(ExcerptTransform.PHRASE_INDEXES_ATTRIBUTE)).andReturn(true);
        @SuppressWarnings("rawtypes")
        Attribute phraseIndexAttribute = new Content(phraseIndexes.toString(), new Key(), false);
        // noinspection unchecked
        expect(document.get(ExcerptTransform.PHRASE_INDEXES_ATTRIBUTE)).andReturn(phraseIndexAttribute);
        
        Key metadata = new Key("Row", "cf", "cq");
        expect(document.getMetadata()).andReturn(metadata).times(2);
        
        givenDocument(document);
    }
    
    private void givenDocument(Document document) {
        this.document = document;
    }
    
    private Map.Entry<Key,Document> getDocumentEntry() {
        return new AbstractMap.SimpleEntry<>(new Key(), document);
    }
    
    private void givenExcerptField(String field, int offset) {
        excerptFields.put(field, offset);
    }
    
    private void givenPhraseIndex(String field, int start, int end) {
        phraseIndexes.addIndexPair(field, start, end);
    }
    
    private void givenMatchingPhrase(String field, int start, int end, String phrase) throws IOException {
        Map<String,String> options = getOptions(field, start, end);
        iterator.init(source, options, env);
        iterator.seek(anyObject(), anyObject(), eq(false));
        if (phrase != null) {
            expect(iterator.hasTop()).andReturn(true);
            Key key = new Key(new Text("row"), new Text("cf"), new Text(field + Constants.NULL + phrase));
            expect(iterator.getTopKey()).andReturn(key);
        } else {
            expect(iterator.hasTop()).andReturn(false);
        }
    }
    
    private Map<String,String> getOptions(String field, int start, int end) {
        Map<String,String> options = new HashMap<>();
        options.put(TermFrequencyExcerptIterator.FIELD_NAME, field);
        options.put(TermFrequencyExcerptIterator.START_OFFSET, String.valueOf(start));
        options.put(TermFrequencyExcerptIterator.END_OFFSET, String.valueOf(end));
        return options;
    }
}
