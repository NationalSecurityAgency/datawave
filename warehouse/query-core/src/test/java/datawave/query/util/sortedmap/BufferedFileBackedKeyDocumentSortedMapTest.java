package datawave.query.util.sortedmap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.collections.keyvalue.UnmodifiableMapEntry;

import datawave.data.type.LcNoDiacriticsType;
import datawave.query.attributes.Document;
import datawave.query.composite.CompositeMetadata;
import datawave.query.predicate.EventDataQueryFieldFilter;
import datawave.query.util.TypeMetadata;
import datawave.query.util.sortedmap.rfile.KeyValueByteDocumenTransformsTest;

public class BufferedFileBackedKeyDocumentSortedMapTest extends BufferedFileBackedRewritableSortedMapTest<Key,Document> {

    private Comparator<Key> keyComparator = new Comparator<>() {
        @Override
        public int compare(Key o1, Key o2) {
            return o1.compareTo(o2);
        }
    };

    private FileSortedMap.RewriteStrategy<Key,Document> keyValueComparator = new FileSortedMap.RewriteStrategy<>() {
        @Override
        public boolean rewrite(Key key, Document original, Document update) {
            long ts1 = original.get(Document.DOCKEY_FIELD_NAME).getTimestamp();
            long ts2 = update.get(Document.DOCKEY_FIELD_NAME).getTimestamp();
            return (ts2 > ts1);
        }
    };

    @Override
    public FileSortedMap.RewriteStrategy<Key,Document> getRewriteStrategy() {
        return keyValueComparator;
    }

    @Override
    public Key createKey(byte[] values) {
        return new Key(values);
    }

    @Override
    public Document createValue(byte[] values) {
        Key docKey = new Key("20200101_1", "datatype\u0000uid", "", values[0]);
        Key attrKey = new Key("20200101_1", "datatype\u0000uid", "FIELD\u0000VALUE", values[0]);
        List<Map.Entry<Key,Value>> attrs = new ArrayList<>();
        attrs.add(new UnmodifiableMapEntry(attrKey, new Value()));
        Document doc = new Document(docKey, Collections.singleton(docKey), false, attrs.iterator(),
                        new TypeMetadata().put("FIELD", "datatype", LcNoDiacriticsType.class.getName()), new CompositeMetadata(), true, true,
                        new EventDataQueryFieldFilter());
        return doc;
    }

    @Override
    protected void testEquality(Map.Entry<Key,Document> expected, Map.Entry<Key,Document> value) {
        testEquality(expected.getKey(), value.getKey());
        KeyValueByteDocumenTransformsTest.assertDocumentEquals(expected.getValue(), value.getValue());
    }

    @Override
    public Comparator<Key> getComparator() {
        return keyComparator;
    }

    @Override
    public FileSortedMap.FileSortedMapFactory<Key,Document> getFactory() {
        return new FileKeyDocumentSortedMap.Factory();
    }
}
