package nsa.datawave.query.rewrite.iterator.profile;

import nsa.datawave.query.rewrite.attributes.Document;
import nsa.datawave.query.rewrite.iterator.NestedIterator;
import nsa.datawave.query.rewrite.iterator.SeekableIterator;
import nsa.datawave.query.rewrite.iterator.SeekableNestedIterator;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Range;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collection;

public class EvaluationTrackingNestedIterator<T> extends SeekableNestedIterator<T> {
    
    protected QuerySpan mySpan;
    protected QuerySpan.Stage stageName;
    private Logger log = Logger.getLogger(EvaluationTrackingNestedIterator.class);
    
    public EvaluationTrackingNestedIterator(QuerySpan.Stage stageName, QuerySpan mySpan, NestedIterator<T> itr) {
        super(itr);
        this.mySpan = mySpan;
        this.stageName = stageName;
    }
    
    @Override
    public T next() {
        long start = System.currentTimeMillis();
        T next = super.next();
        mySpan.addStageTimer(stageName, System.currentTimeMillis() - start);
        return next;
    }
    
    @Override
    public T move(T minimum) {
        long start = System.currentTimeMillis();
        T next = super.move(minimum);
        mySpan.addStageTimer(stageName, System.currentTimeMillis() - start);
        return next;
    }
    
    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        long start = System.currentTimeMillis();
        super.seek(range, columnFamilies, inclusive);
        mySpan.addStageTimer(stageName, System.currentTimeMillis() - start);
    }
}
