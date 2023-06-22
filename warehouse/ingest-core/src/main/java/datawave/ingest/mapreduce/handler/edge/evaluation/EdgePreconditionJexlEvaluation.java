package datawave.ingest.mapreduce.handler.edge.evaluation;

import com.google.common.base.Predicate;
import org.apache.commons.jexl2.JexlArithmetic;
import org.apache.commons.jexl2.Script;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Collection;

/**
 *
 * This class operates against a {@code Multimap<String, NormalizedContentInterface>} normalizedFields during ingest.
 *
 */
public class EdgePreconditionJexlEvaluation implements Predicate<Script> {

    private static final Logger log = LoggerFactory.getLogger(EdgePreconditionJexlEvaluation.class);

    private EdgePreconditionJexlContext jexlContext;

    private JexlArithmetic arithmetic;

    public EdgePreconditionJexlEvaluation() {}

    public EdgePreconditionJexlEvaluation(EdgePreconditionJexlContext jexlContext) {
        setJexlContext(jexlContext);
    }

    /**
     * This convenience method can be used to interpret the result of the script.execute() result which calls the interpret method below.
     *
     * @param scriptExecuteResult
     *            the result of script execute
     * @return true if we matched, false otherwise.
     */
    public boolean isMatched(Object scriptExecuteResult) {
        boolean matched = false;
        if (scriptExecuteResult != null && Boolean.class.isAssignableFrom(scriptExecuteResult.getClass())) {
            matched = (Boolean) scriptExecuteResult;
        } else if (scriptExecuteResult != null && Collection.class.isAssignableFrom(scriptExecuteResult.getClass())) {
            // if the function returns a collection of matches, return true/false
            // based on the number of matches
            Collection<?> matches = (Collection<?>) scriptExecuteResult;
            matched = (!matches.isEmpty());
        } else if (log.isDebugEnabled()) {
            log.debug("Unable to process non-Boolean result from JEXL evaluation '" + scriptExecuteResult);
        }
        return matched;
    }

    @Override
    public boolean apply(Script compiledScript) {
        boolean matched = false;
        if (null == getJexlContext()) {
            log.trace("Dropping entry because it was null");

            return false;
        }

        Object o = compiledScript.execute(getJexlContext());
        return isMatched(o);

    }

    public EdgePreconditionJexlContext getJexlContext() {
        return jexlContext;
    }

    public void setJexlContext(EdgePreconditionJexlContext jexlContext) {
        this.jexlContext = jexlContext;
    }

}
