package datawave.query.iterator.profile;

import datawave.query.iterator.NestedIterator;
import datawave.query.iterator.SeekableNestedIterator;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collection;

public class EvaluationTrackingNestedIterator<T> extends SeekableNestedIterator<T> {

    protected QuerySpan mySpan;
    protected QuerySpan.Stage stageName;
    private Logger log = Logger.getLogger(EvaluationTrackingNestedIterator.class);

    public EvaluationTrackingNestedIterator(QuerySpan.Stage stageName, QuerySpan mySpan, NestedIterator<T> itr, IteratorEnvironment env) {
        super(itr, env);
        this.mySpan = mySpan;
        this.stageName = stageName;
    }

    @Override
    public T next() {
        mySpan.next();
        long start = System.currentTimeMillis();
        T next = super.next();
        mySpan.addStageTimer(stageName, System.currentTimeMillis() - start);
        return next;
    }

    @Override
    public T move(T minimum) {
        mySpan.seek();
        long start = System.currentTimeMillis();
        T next = super.move(minimum);
        mySpan.addStageTimer(stageName, System.currentTimeMillis() - start);
        return next;
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        mySpan.seek();
        long start = System.currentTimeMillis();
        super.seek(range, columnFamilies, inclusive);
        mySpan.addStageTimer(stageName, System.currentTimeMillis() - start);
    }
}
