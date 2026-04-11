package datawave.query.function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.junit.Test;

import com.google.common.collect.Lists;

import datawave.query.attributes.Document;
import datawave.query.attributes.DocumentKey;
import datawave.query.jexl.functions.DocumentFunctions;
import datawave.query.util.Tuple3;
import datawave.query.util.Tuples;

/**
 * Focused tests for {@link DocumentMatchContextFunction}.
 */
public class DocumentMatchContextFunctionTest {

    /**
     * Verifies that only matching {@code d}-column entries for the current document key are added to the evaluation side-channel.
     */
    @Test
    public void testCollectsOnlyCurrentDocumentColumns() {
        List<Map.Entry<Key,Value>> entries = Lists.newArrayList(Map.entry(new Key("20240101_0", "d", "datatype\0uid\0BODY"), new Value("one".getBytes())),
                        Map.entry(new Key("20240101_0", "d", "datatype\0uid\0META"), new Value("two".getBytes())),
                        Map.entry(new Key("20240101_0", "d", "datatype\0other\0BODY"), new Value("skip".getBytes())),
                        Map.entry(new Key("20240101_0", "tf", "datatype\0uid\0BODY"), new Value("skip".getBytes())));

        DocumentMatchConfig config = new DocumentMatchConfig();
        config.setSource(new ListBackedIterator(entries));
        config.setLimits(new DocumentMatchContext.Limits(1234, 5678, 9012));
        DocumentMatchContextFunction function = new DocumentMatchContextFunction(config);

        Tuple3<Key,Document,Map<String,Object>> result = function
                        .apply(Tuples.tuple(new Key("20240101_0", "datatype\0uid"), new Document(), Collections.emptyMap()));
        DocumentMatchContext context = (DocumentMatchContext) result.third().get(DocumentFunctions.DOCUMENT_MATCH_CONTEXT_JEXL_VARIABLE_NAME);

        assertEquals(2, context.getdEntries().size());
        assertEquals(1234, context.getMaxEncodedValueSize());
        assertEquals(5678, context.getMaxDecodedValueSize());
        assertEquals(9012, context.getMaxEncodedContextSize());
    }

    /**
     * Verifies that the function produces an empty context entry when a document has no retained {@code d}-column values.
     */
    @Test
    public void testCollectsEmptyContextWhenNoDocumentColumnsExist() {
        DocumentMatchConfig config = new DocumentMatchConfig();
        config.setSource(new ListBackedIterator(Collections.emptyList()));
        config.setLimits(new DocumentMatchContext.Limits(10, 20, 30));
        DocumentMatchContextFunction function = new DocumentMatchContextFunction(config);

        Tuple3<Key,Document,Map<String,Object>> result = function
                        .apply(Tuples.tuple(new Key("20240101_0", "datatype\0uid"), new Document(), Collections.emptyMap()));
        DocumentMatchContext context = (DocumentMatchContext) result.third().get(DocumentFunctions.DOCUMENT_MATCH_CONTEXT_JEXL_VARIABLE_NAME);

        assertTrue(context.getdEntries().isEmpty());
    }

    /**
     * Verifies that document-match context collection honors explicit {@code DOCKEY} attributes instead of assuming that the tuple key is the only event key.
     */
    @Test
    public void testCollectsColumnsForDocumentKeysFromDocument() {
        List<Map.Entry<Key,Value>> entries = Lists.newArrayList(Map.entry(new Key("20240101_0", "d", "datatype\0uid\0BODY"), new Value("one".getBytes())),
                        Map.entry(new Key("20240101_0", "d", "datatype\0child\0BODY"), new Value("two".getBytes())),
                        Map.entry(new Key("20240101_0", "d", "datatype\0other\0BODY"), new Value("skip".getBytes())));

        DocumentMatchConfig config = new DocumentMatchConfig();
        config.setSource(new ListBackedIterator(entries));
        config.setLimits(new DocumentMatchContext.Limits(10, 20, 30));
        config.setTld(true);
        DocumentMatchContextFunction function = new DocumentMatchContextFunction(config);

        Document document = new Document();
        document.put(Document.DOCKEY_FIELD_NAME, new DocumentKey(new Key("20240101_0", "datatype\0uid"), false));
        document.put(Document.DOCKEY_FIELD_NAME, new DocumentKey(new Key("20240101_0", "datatype\0child"), false));

        Tuple3<Key,Document,Map<String,Object>> result = function
                        .apply(Tuples.tuple(new Key("20240101_0", "datatype\0root"), document, Collections.emptyMap()));
        DocumentMatchContext context = (DocumentMatchContext) result.third().get(DocumentFunctions.DOCUMENT_MATCH_CONTEXT_JEXL_VARIABLE_NAME);

        assertEquals(2, context.getdEntries().size());
    }

    /**
     * Verifies that collection skips individually oversized payloads and stops once the retained encoded bytes would exceed the configured aggregate limit.
     */
    @Test
    public void testCollectsOnlyEntriesWithinAggregateEncodedContextLimit() {
        List<Map.Entry<Key,Value>> entries = Lists.newArrayList(Map.entry(new Key("20240101_0", "d", "datatype\0uid\0BODY"), new Value("1234".getBytes())),
                        Map.entry(new Key("20240101_0", "d", "datatype\0uid\0META"), new Value("12345".getBytes())),
                        Map.entry(new Key("20240101_0", "d", "datatype\0uid\0TAIL"), new Value("12".getBytes())));

        DocumentMatchConfig config = new DocumentMatchConfig();
        config.setSource(new ListBackedIterator(entries));
        config.setLimits(new DocumentMatchContext.Limits(10, 20, 4));
        DocumentMatchContextFunction function = new DocumentMatchContextFunction(config);

        Tuple3<Key,Document,Map<String,Object>> result = function
                        .apply(Tuples.tuple(new Key("20240101_0", "datatype\0uid"), new Document(), Collections.emptyMap()));
        DocumentMatchContext context = (DocumentMatchContext) result.third().get(DocumentFunctions.DOCUMENT_MATCH_CONTEXT_JEXL_VARIABLE_NAME);

        assertEquals(1, context.getdEntries().size());
        assertEquals("datatype\0uid\0BODY", context.getdEntries().get(0).getKey().getColumnQualifier().toString());
    }

    private static class ListBackedIterator implements SortedKeyValueIterator<Key,Value> {
        private final List<Map.Entry<Key,Value>> entries;
        private int index = -1;

        private ListBackedIterator(List<Map.Entry<Key,Value>> entries) {
            this.entries = entries;
        }

        @Override
        public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) {}

        @Override
        public boolean hasTop() {
            return index >= 0 && index < entries.size();
        }

        @Override
        public void next() {
            index++;
        }

        @Override
        public void seek(Range range, java.util.Collection<org.apache.accumulo.core.data.ByteSequence> columnFamilies, boolean inclusive) {
            index = 0;
            while (index < entries.size() && !range.contains(entries.get(index).getKey())) {
                index++;
            }
        }

        @Override
        public Key getTopKey() {
            return entries.get(index).getKey();
        }

        @Override
        public Value getTopValue() {
            return entries.get(index).getValue();
        }

        @Override
        public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
            return new ListBackedIterator(entries);
        }
    }
}
