package datawave.query.tables.facets;

import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import datawave.query.function.MergeSummarization;
import datawave.query.function.deserializer.DocumentDeserializer;
import datawave.query.attributes.Document;
import datawave.query.function.serializer.DocumentSerializer;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;

/**
 *
 */
public class FacetedFunction implements Function<Entry<Key,Value>,Entry<Key,Value>> {

    protected DocumentDeserializer deserializer;
    protected DocumentSerializer serializer;
    protected List<Function<Entry<Key,Document>,Entry<Key,Document>>> transforms;
    protected Document mergedDocment = new Document();
    private MergeSummarization summarizer;

    public FacetedFunction(DocumentDeserializer deserializer, DocumentSerializer serializer,
                    List<Function<Entry<Key,Document>,Entry<Key,Document>>> transforms) {
        this.deserializer = deserializer;
        this.serializer = serializer;
        this.transforms = transforms;
        summarizer = null;
    }

    /*
     * (non-Javadoc)
     *
     * @see rx.functions.Action1#call(java.lang.Object)
     */
    @Override
    public Entry<Key,Value> apply(Entry<Key,Value> entry) {

        Entry<Key,Document> doc = deserializer.apply(entry);

        if (null == summarizer) {
            summarizer = new MergeSummarization(doc.getKey(), doc.getValue());
        }
        Iterator<Entry<Key,Document>> finalIter = Iterators.singletonIterator(summarizer.apply(doc));

        for (Function<Entry<Key,Document>,Entry<Key,Document>> func : transforms) {
            finalIter = Iterators.transform(finalIter, func);
        }

        return serializer.apply(finalIter.next());

    }

}
