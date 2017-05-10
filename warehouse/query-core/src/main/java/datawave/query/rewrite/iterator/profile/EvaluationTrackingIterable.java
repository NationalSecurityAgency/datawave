package datawave.query.rewrite.iterator.profile;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map.Entry;
import datawave.query.rewrite.attributes.Document;
import datawave.query.rewrite.iterator.AccumuloTreeIterable;
import datawave.query.rewrite.iterator.aggregation.DocumentData;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.log4j.Logger;

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
