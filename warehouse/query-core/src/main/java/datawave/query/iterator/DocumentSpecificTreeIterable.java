package datawave.query.iterator;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import datawave.query.attributes.Document;
import datawave.query.iterator.aggregation.DocumentData;

/**
 *
 */
public class DocumentSpecificTreeIterable extends AccumuloTreeIterable<Key,DocumentData> {
    private final Key documentKey;

    public DocumentSpecificTreeIterable(Key documentKey, Function<Entry<Key,Document>,Entry<DocumentData,Document>> func) {
        super();

        Preconditions.checkNotNull(documentKey);

        this.documentKey = documentKey;
        this.func = func;
    }

    public Iterator<Entry<DocumentData,Document>> iterator() {
        Entry<DocumentData,Document> entry = this.func.apply(Maps.immutableEntry(this.documentKey, new Document()));

        return Collections.singleton(entry).iterator();
    }

    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        // noop?
    }
}
