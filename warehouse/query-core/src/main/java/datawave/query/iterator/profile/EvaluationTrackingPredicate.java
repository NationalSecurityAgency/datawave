package datawave.query.iterator.profile;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import com.google.common.base.Predicate;

public class EvaluationTrackingPredicate<T> implements Predicate<T> {

    protected QuerySpan mySpan;
    protected QuerySpan.Stage stageName;
    protected Predicate<T> predicate;
    private Logger log = LogManager.getLogger(EvaluationTrackingPredicate.class);

    public EvaluationTrackingPredicate(QuerySpan.Stage stageName, QuerySpan mySpan, Predicate<T> predicate) {
        this.mySpan = mySpan;
        this.stageName = stageName;
        this.predicate = predicate;
    }

    @Override
    public boolean apply(T input) {
        long start = System.currentTimeMillis();
        boolean output = predicate.apply(input);
        mySpan.addStageTimer(stageName, System.currentTimeMillis() - start);
        return output;
    }
}
