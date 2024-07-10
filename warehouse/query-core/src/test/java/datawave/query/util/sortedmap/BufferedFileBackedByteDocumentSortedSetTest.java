package datawave.query.util.sortedmap;

import datawave.data.type.LcNoDiacriticsType;
import datawave.query.attributes.Document;
import datawave.query.composite.CompositeMetadata;
import datawave.query.predicate.EventDataQueryFieldFilter;
import datawave.query.predicate.KeyProjection;
import datawave.query.util.TypeMetadata;
import datawave.query.util.sortedset.ByteArrayComparator;
import datawave.query.util.sortedset.FileSortedSet;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.collections.keyvalue.UnmodifiableMapEntry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class BufferedFileBackedByteDocumentSortedSetTest extends BufferedFileBackedRewritableSortedSetTest<byte[],Document> {

    private Comparator<Map.Entry<byte[],Document>> keyComparator = new Comparator<>() {
        private Comparator<byte[]> comparator = new ByteArrayComparator();

        @Override
        public int compare(Map.Entry<byte[],Document> o1, Map.Entry<byte[],Document> o2) {
            return comparator.compare(o1.getKey(), o2.getKey());
        }
    };

    private RewritableSortedSetImpl.RewriteStrategy<Map.Entry<byte[],Document>> keyValueComparator = new RewritableSortedSetImpl.RewriteStrategy<>() {
        @Override
        public boolean rewrite(Map.Entry<byte[],Document> original, Map.Entry<byte[],Document> update) {
            int comparison = keyComparator.compare(original, update);
            if (comparison == 0) {
                long ts1 = original.getValue().get(Document.DOCKEY_FIELD_NAME).getTimestamp();
                long ts2 = update.getValue().get(Document.DOCKEY_FIELD_NAME).getTimestamp();
                return (ts2 > ts1);
            }
            return comparison < 0;
        }
    };

    @Override
    public RewritableSortedSet.RewriteStrategy<Map.Entry<byte[],Document>> getRewriteStrategy() {
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
        assertEquals(0, keyComparator.compare(expected, value));
        assertEquals(expected.getValue().get(Document.DOCKEY_FIELD_NAME), value.getValue().get(Document.DOCKEY_FIELD_NAME));
    }

    @Override
    public Comparator<Map.Entry<byte[],Document>> getComparator() {
        return keyComparator;
    }

    @Override
    public FileSortedMap.FileSortedMapFactory<Map.Entry<byte[],Document>> getFactory() {
        return new FileByteDocumentSortedMap.Factory();
    }
}
