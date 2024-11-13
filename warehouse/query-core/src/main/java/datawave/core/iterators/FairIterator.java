package datawave.core.iterators;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.log4j.Logger;

/**
 *
 */
public class FairIterator extends WrappingIterator {

    private static final Logger log = Logger.getLogger(FairIterator.class);
    /**
     * Maximum consecutive keys visited.
     */
    public static final String MAX_SESSION_TIME = "max.session.time";

    protected static final Value EXCEPTEDVALUE = new Value(new byte[] {0x0d, 0x0e, 0x0a, 0x0d, 0x0b, 0x0e, 0x0e, 0x0f});

    /**
     * Maximum consecutive keys visited.
     */
    protected long maxSessionTime = Long.MAX_VALUE;

    private Range myRange;

    // the following are threadlocal so that they can be shared within the same thread
    // but at different points throughout the query chain if this is desired

    protected boolean exceededTime = false;

    protected long currentSession = -1;

    private Key myKey = new Key();

    public static boolean exceededFairValue(Entry<Key,Value> kv) {
        return kv.getValue().equals(EXCEPTEDVALUE);
    }

    @Override
    public boolean hasTop() {

        if (!isExceededTime()) {
            boolean top = super.hasTop();

            if (isUnfairExecution())
                setExceedtime();

            return top;
        } else {
            if (myKey != null)
                return true;
            else
                return false;
        }
    }

    @Override
    public Key getTopKey() {
        if (!isExceededTime()) {
            return super.getTopKey();
        } else {
            return myKey;
        }
    }

    @Override
    public Value getTopValue() {
        if (!isExceededTime()) {
            return super.getTopValue();
        } else {
            return EXCEPTEDVALUE;

        }
    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        final String maxKeysOpt = options.get(MAX_SESSION_TIME);
        if (null != maxKeysOpt) {
            try {
                maxSessionTime = Long.parseLong(maxKeysOpt);
            } catch (NumberFormatException nfe) {
                if (log.isTraceEnabled()) {
                    log.trace("Defaulting to Long.MAX_VALUE since maxKeysVisit is an invalid long value");
                }
            }
        }
        exceededTime = false;
        currentSession = System.currentTimeMillis();
        myKey = null;
    }

    @Override
    public void next() throws IOException {

        if (isExceededTime()) {
            if (null != myKey)
                myKey = null;
            return;
        }

        if (isUnfairExecution()) {
            setExceedtime();
            return;
        }

        super.next();

    }

    protected boolean isUnfairExecution() {
        return ((System.currentTimeMillis() - currentSession) >= maxSessionTime);
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        myRange = range;

        if (isExceededTime())
            return;

        if (isUnfairExecution()) {
            setExceedtime();
            return;
        }

        super.seek(range, columnFamilies, inclusive);

        if (isUnfairExecution()) {
            setExceedtime();
        }
    }

    protected boolean isExceededTime() {

        return exceededTime;
    }

    protected void setExceedtime() {
        exceededTime = true;
        Key topKey = myRange.getStartKey();
        if (!myRange.isStartKeyInclusive()) {
            topKey = topKey.followingKey(PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME);

        }
        if (myRange.isInfiniteStartKey())
            topKey = new Key();

        myKey = topKey;
    }

    public class FairnessHaltException extends IOException {

        /**
         * @param message
         *            a message
         */
        public FairnessHaltException(String message) {
            super(message);
        }

        private static final long serialVersionUID = -5347155393215490030L;

    }
}
