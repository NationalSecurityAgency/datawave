package datawave.query.iterator.profile;

import java.util.Iterator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class EvaluationTrackingIterator<T> implements Iterator<T> {

    protected QuerySpan mySpan;
    protected QuerySpan.Stage stageName;
    private Logger log = LogManager.getLogger(EvaluationTrackingIterator.class);
    private Iterator<T> itr;

    public EvaluationTrackingIterator(QuerySpan.Stage stageName, QuerySpan mySpan, Iterator<T> itr) {
        this.itr = itr;
        this.mySpan = mySpan;
        this.stageName = stageName;
    }

    @Override
    public boolean hasNext() {
        return itr.hasNext();
    }

    @Override
    public T next() {
        long start = System.currentTimeMillis();
        T next = itr.next();
        mySpan.addStageTimer(stageName, System.currentTimeMillis() - start);
        return next;

    }

    @Override
    public void remove() {
        this.itr.remove();
    }
}
