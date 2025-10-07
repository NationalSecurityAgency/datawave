package datawave.query.iterator.profile;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.log4j.Logger;

import datawave.query.attributes.Document;
import datawave.query.iterator.AccumuloTreeIterable;
import datawave.query.iterator.aggregation.DocumentData;

public class EvaluationTrackingIterable extends AccumuloTreeIterable<Key,DocumentData> {

    protected QuerySpan mySpan;
    private Logger log = Logger.getLogger(EvaluationTrackingIterable.class);
    private Iterator<Entry<DocumentData,Document>> itr = null;
    private AccumuloTreeIterable<Key,DocumentData> iterable;
    private QuerySpan.Stage stageName = null;

    public EvaluationTrackingIterable(QuerySpan.Stage stageName, QuerySpan mySpan, AccumuloTreeIterable<Key,DocumentData> iterable) {
        this.mySpan = mySpan;
        this.iterable = iterable;
        this.stageName = stageName;
    }

    @Override
    public Iterator<Entry<DocumentData,Document>> iterator() {
        return new EvaluationTrackingIterator<>(stageName, mySpan, iterable.iterator());
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        long start = System.currentTimeMillis();
        iterable.seek(range, columnFamilies, inclusive);
        mySpan.addStageTimer(stageName, System.currentTimeMillis() - start);
    }
}
