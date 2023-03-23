package datawave.query.iterator;

import static datawave.query.function.LogTiming.TIMING_METADATA;
import static datawave.query.iterator.waitwindow.WaitWindowObserver.WAIT_WINDOW_OVERRUN;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.YieldCallback;
import org.apache.log4j.Logger;

import datawave.query.attributes.Document;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.iterator.waitwindow.WaitWindowObserver;
import datawave.query.util.TypeMetadata;

public class TestLookupTask<T extends QueryIterator> {

    private static final Logger log = Logger.getLogger(TestLookupTask.class);
    private TypeMetadata typeMetadata = null;
    private Class<T> iteratorClass;
    private QueryIterator iterator;

    public TestLookupTask(Class<T> iteratorClass) {
        this.iteratorClass = iteratorClass;
    }

    public void setTypeMetadata(TypeMetadata typeMetadata) {
        this.typeMetadata = typeMetadata;
    }

    private QueryIterator init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env, YieldCallback yield)
                    throws IOException {
        try {
            QueryIterator iter = this.iteratorClass.newInstance();
            iter.setTypeMetadata(this.typeMetadata);
            iter.init(source, options, env);
            iter.enableYielding(yield);
            return iter;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    public List<Map.Entry<Key,Document>> lookup(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env,
                    List<Range> ranges) throws IOException {
        List<Map.Entry<Key,Document>> results = new ArrayList<>();
        YieldCallback<Key> yield = new YieldCallback();
        this.iterator = init(source, options, env, yield);
        for (Range range : ranges) {
            boolean rangeCompleted = false;
            Range r = range;
            while (!rangeCompleted) {
                log.trace("Seeking to range:" + r);
                this.iterator.seek(r, Collections.EMPTY_LIST, false);
                while (this.iterator.hasTop()) {
                    Document document = deserializeAndFilterDocument(this.iterator.getTopValue());
                    if (document.getDictionary().size() > 0) {
                        results.add(new AbstractMap.SimpleEntry(this.iterator.getTopKey(), document));
                    } else {
                        log.trace("Filtering out invalid document");
                    }
                    this.iterator.next();
                }
                if (yield.hasYielded()) {
                    Key yieldKey = yield.getPositionAndReset();
                    if (!r.contains(yieldKey)) {
                        throw new IllegalStateException("Yielded to key outside of range " + yieldKey + " not in " + r);
                    }
                    if (!results.isEmpty()) {
                        Key lastResultKey = results.get(results.size() - 1).getKey();
                        if (lastResultKey != null && yieldKey.compareTo(lastResultKey) <= 0) {
                            throw new IllegalStateException("Underlying iterator yielded to a position that does not follow the last key returned: " + yieldKey
                                            + " <= " + lastResultKey);
                        }
                    }
                    log.debug("Yielded at " + yieldKey + " after seeking range " + r);
                    yield = new YieldCallback<>();
                    this.iterator = init(source, options, env, yield);
                    r = new Range(yieldKey, false, r.getEndKey(), r.isEndKeyInclusive());
                } else {
                    rangeCompleted = true;
                }
            }
        }
        return results;
    }

    private Document deserializeAndFilterDocument(Value value) {
        Map.Entry<Key,Document> deserializedValue = deserialize(value);
        Document d = deserializedValue.getValue();
        Document filteredDocument = new Document();
        d.getDictionary().entrySet().stream().forEach(e -> {
            if (!e.getKey().equals(TIMING_METADATA) && !e.getKey().equals(WAIT_WINDOW_OVERRUN)) {
                filteredDocument.put(e.getKey(), e.getValue());
            }
        });
        return filteredDocument;
    }

    private Map.Entry<Key,Document> deserialize(Value value) {
        KryoDocumentDeserializer dser = new KryoDocumentDeserializer();
        return dser.apply(new AbstractMap.SimpleEntry(null, value));
    }
}
