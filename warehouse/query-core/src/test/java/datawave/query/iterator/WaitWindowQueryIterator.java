package datawave.query.iterator;

import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WaitWindowQueryIterator extends QueryIterator {

    private static Logger log = LoggerFactory.getLogger(WaitWindowQueryIterator.class);

    public WaitWindowQueryIterator() {
        this(5, 2);
    }

    public WaitWindowQueryIterator(long maxChecksBeforeYield, long randomYieldFrequency) {
        super();
        this.waitWindowObserver = new TestWaitWindowObserver(maxChecksBeforeYield, randomYieldFrequency);
    }

    public WaitWindowQueryIterator(WaitWindowQueryIterator other, IteratorEnvironment env) {
        super(other, env);
        this.waitWindowObserver = new TestWaitWindowObserver((TestWaitWindowObserver) other.waitWindowObserver);
    }
}
