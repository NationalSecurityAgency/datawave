package datawave.query.util.sortedmap;

import static org.junit.Assert.assertEquals;

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
import datawave.query.predicate.KeyProjection;
import datawave.query.util.TypeMetadata;
import datawave.query.util.sortedset.ByteArrayComparator;

public class BufferedFileBackedByteDocumentSortedMapTest extends BufferedFileBackedRewritableSortedMapTest<byte[],Document> {

    private Comparator<byte[]> keyComparator = new ByteArrayComparator();

    private FileSortedMap.RewriteStrategy<byte[],Document> keyValueComparator = new FileSortedMap.RewriteStrategy<>() {
        @Override
        public boolean rewrite(byte[] key, Document original, Document update) {
            long ts1 = original.get(Document.DOCKEY_FIELD_NAME).getTimestamp();
            long ts2 = update.get(Document.DOCKEY_FIELD_NAME).getTimestamp();
            return (ts2 > ts1);
        }
    };

    @Override
    public FileSortedMap.RewriteStrategy<byte[],Document> getRewriteStrategy() {
        return keyValueComparator;
    }

    @Override
    public byte[] createKey(byte[] values) {
        return values;
    }

    @Override
    public Document createValue(byte[] values) {
        Key docKey = new Key("20200101_1", "datatype\u0000uid", "", values[0]);
        Key attrKey = new Key("20200101_1", "datatype\u0000uid", "FIELD\u0000VALUE", values[0]);
        List<Map.Entry<Key,Value>> attrs = new ArrayList<>();
        attrs.add(new UnmodifiableMapEntry(attrKey, new Value()));
        Document doc = new Document(docKey, Collections.singleton(docKey), false, attrs.iterator(),
                        new TypeMetadata().put("FIELD", "datatype", LcNoDiacriticsType.class.getName()), new CompositeMetadata(), true, true,
                        new EventDataQueryFieldFilter(new KeyProjection()));
        return doc;
    }

    @Override
    public void testFullEquality(Map.Entry<byte[],Document> expected, Map.Entry<byte[],Document> value) {
        assertEquals(0, keyComparator.compare(expected.getKey(), value.getKey()));
        assertEquals(expected.getValue().get(Document.DOCKEY_FIELD_NAME), value.getValue().get(Document.DOCKEY_FIELD_NAME));
    }

    @Override
    public Comparator<byte[]> getComparator() {
        return keyComparator;
    }

    @Override
    public FileSortedMap.FileSortedMapFactory<byte[],Document> getFactory() {
        return new FileByteDocumentSortedMap.Factory();
    }
}
