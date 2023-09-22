package datawave.core.iterators;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.log4j.Logger;

/**
 * Purpose: Enforces a timeout within this scan session
 *
 * Design: Upon calls to next, seek, and even init ( in the case where we have filters ) we will check if we have reached an unfair execution event in which we
 * have exceed our timeout
 */
public class TimeoutIterator extends WrappingIterator {

    private static final Logger log = Logger.getLogger(TimeoutIterator.class);
    /**
     * Maximum consecutive keys visited.
     */
    public static final String MAX_SESSION_TIME = "max.session.time";

    /**
     * Maximum consecutive keys visited.
     */
    protected long maxSessionTime = Long.MAX_VALUE;

    protected long currentSession = -1;

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        TimeoutIterator to = new TimeoutIterator();
        to.setSource(getSource().deepCopy(env));
        to.maxSessionTime = maxSessionTime;
        to.currentSession = currentSession;
        return to;
    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);

        final String maxSessionOpt = options.get(MAX_SESSION_TIME);
        if (null != maxSessionOpt) {
            try {
                maxSessionTime = Long.parseLong(maxSessionOpt);
            } catch (NumberFormatException nfe) {
                if (log.isTraceEnabled()) {
                    log.trace("Defaulting to Long.MAX_VALUE since maxKeysVisit is an invalid long value");
                }
            }
        }

        currentSession = System.currentTimeMillis();

    }

    @Override
    public void next() throws IOException {

        if (isUnfairExecution()) {

            throw new IteratorTimeoutException("Exception next()");
        }

        super.next();

    }

    /**
     * identifies if we have exceeded the time prescribed by max.session.time
     *
     * @return whether the time has been exceeded
     */
    protected boolean isUnfairExecution() {
        return ((System.currentTimeMillis() - currentSession) >= maxSessionTime);
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {

        if (isUnfairExecution()) {
            throw new IteratorTimeoutException("Exception seek()");
        }

        super.seek(range, columnFamilies, inclusive);

    }

}
