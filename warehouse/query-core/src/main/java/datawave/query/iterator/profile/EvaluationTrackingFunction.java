package datawave.query.iterator.profile;

import com.google.common.base.Function;
import org.apache.log4j.Logger;

public class EvaluationTrackingFunction<F,T> implements Function<F,T> {

    protected QuerySpan mySpan;
    protected QuerySpan.Stage stageName;
    protected Function<F,T> function;
    private Logger log = Logger.getLogger(EvaluationTrackingFunction.class);

    public EvaluationTrackingFunction(QuerySpan.Stage stageName, QuerySpan mySpan, Function<F,T> function) {
        this.mySpan = mySpan;
        this.stageName = stageName;
        this.function = function;
    }

    @Override
    public T apply(F input) {
        long start = System.currentTimeMillis();
        T output = function.apply(input);
        mySpan.addStageTimer(stageName, System.currentTimeMillis() - start);
        return output;
    }
}
