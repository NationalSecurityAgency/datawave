package datawave.query.index.lookup;

import com.google.common.collect.Maps;
import datawave.ingest.protobuf.Uid;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.util.Tuple2;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.commons.jexl2.parser.JexlNode;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class EntryParserTest {
    
    private static void addToExpected(Collection<IndexMatch> expected, String prefix, Iterable<String> docIds, JexlNode node) {
        for (String docId : docIds)
            expected.add(new IndexMatch(prefix + '\u0000' + docId, node));
    }
    
    /**
     * Assert when EntryParser is set to skip node delays
     */
    @Test
    public void testParse_skipNodeDelay() throws IOException {
        TreeMap<Key,Value> data = new TreeMap<>();
        List<String> docIds = Arrays.asList("doc1", "doc2", "doc3", "doc4");
        Uid.List.Builder builder = Uid.List.newBuilder();
        builder.addAllUID(docIds);
        builder.setCOUNT(docIds.size());
        builder.setIGNORE(false);
        Value hasDocs = new Value(builder.build().toByteArray());
        
        List<IndexMatch> expected = new LinkedList<>();
        
        data.put(new Key("row", "cf", "20190314\u0000A"), hasDocs);
        addToExpected(expected, "A", docIds, JexlNodeFactory.buildEQNode("hello", "world"));
        
        CreateUidsIterator iterator = new CreateUidsIterator();
        iterator.init(new SortedMapIterator(data), null, null);
        iterator.seek(new Range(), Collections.emptySet(), false);
        
        EntryParser parser = new EntryParser("hello", "world", true);
        Map.Entry<Key,Value> top = Maps.immutableEntry(iterator.getTopKey(), iterator.getTopValue());
        Tuple2<String,IndexInfo> tuple = parser.apply(top);
        assertTrue(iterator.hasTop());
        
        assertNotNull(tuple);
        assertEquals("20190314", tuple.first());
        for (IndexMatch match : tuple.second().uids()) {
            assertTrue(expected.remove(match));
        }
        assertTrue(expected.isEmpty());
    }
    
    /**
     * Assert when the IndexInfo has no document ids and the range is a day range
     */
    @Test
    public void testParse_NoDocIds_isDayRange() throws IOException {
        TreeMap<Key,Value> data = new TreeMap<>();
        Uid.List.Builder builder = Uid.List.newBuilder();
        builder.setCOUNT(30);
        builder.setIGNORE(true);
        Value hasDocs = new Value(builder.build().toByteArray());
        
        List<IndexMatch> expected = new LinkedList<>();
        
        data.put(new Key("row", "cf", "20190314\u0000A"), hasDocs);
        List<String> docIds = Arrays.asList("doc1", "doc2", "doc3", "doc4");
        addToExpected(expected, "A", docIds, JexlNodeFactory.buildEQNode("hello", "world"));
        
        CreateUidsIterator iterator = new CreateUidsIterator();
        iterator.init(new SortedMapIterator(data), null, null);
        iterator.seek(new Range(), Collections.emptySet(), false);
        assertTrue(iterator.hasTop());
        
        EntryParser parser = new EntryParser("hello", "world", false);
        Map.Entry<Key,Value> top = Maps.immutableEntry(iterator.getTopKey(), iterator.getTopValue());
        Tuple2<String,IndexInfo> tuple = parser.apply(top);
        
        assertNotNull(tuple);
        assertEquals("20190314", tuple.first());
        assertEquals(0, tuple.second().uids().size());
        assertEquals("((_Delayed_ = true) && (hello == 'world'))", JexlStringBuildingVisitor.buildQuery(tuple.second().getNode()));
    }
    
    /**
     * Assert when the IndexInfo has no document ids and the range is a day range
     */
    @Test
    public void testParse_NoDocIds_isShardRange() throws IOException {
        TreeMap<Key,Value> data = new TreeMap<>();
        Uid.List.Builder builder = Uid.List.newBuilder();
        builder.setCOUNT(30);
        builder.setIGNORE(true);
        Value hasDocs = new Value(builder.build().toByteArray());
        
        List<IndexMatch> expected = new LinkedList<>();
        
        data.put(new Key("row", "cf", "20190314_0\u0000A"), hasDocs);
        List<String> docIds = Arrays.asList("doc1", "doc2", "doc3", "doc4");
        addToExpected(expected, "A", docIds, JexlNodeFactory.buildEQNode("hello", "world"));
        
        CreateUidsIterator iterator = new CreateUidsIterator();
        iterator.init(new SortedMapIterator(data), null, null);
        iterator.seek(new Range(), Collections.emptySet(), false);
        assertTrue(iterator.hasTop());
        
        EntryParser parser = new EntryParser("hello", "world", false);
        Map.Entry<Key,Value> top = Maps.immutableEntry(iterator.getTopKey(), iterator.getTopValue());
        Tuple2<String,IndexInfo> tuple = parser.apply(top);
        
        assertNotNull(tuple);
        assertEquals("20190314_0", tuple.first());
        assertEquals(0, tuple.second().uids().size());
        assertEquals("hello == 'world'", JexlStringBuildingVisitor.buildQuery(tuple.second().getNode()));
    }
}
