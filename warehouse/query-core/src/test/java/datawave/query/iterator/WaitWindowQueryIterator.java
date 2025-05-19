package datawave.query.iterator;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.query.iterator.waitwindow.WaitWindowObserver;

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

    public TestWaitWindowObserver getWaitWindowObserver() {
        return (TestWaitWindowObserver) this.waitWindowObserver;
    }

    // Custom WaitWindowObserver that allows checksBeforeYield checks
    // before either yielding or returning a WAIT_WINDOW_OVERRUN
    public class TestWaitWindowObserver extends WaitWindowObserver {

        protected AtomicLong checksRemainingBeforeYield = new AtomicLong(Long.MAX_VALUE);
        protected long maxChecksBeforeYield;
        protected long randomYieldFrequency;
        protected boolean isStarted = false;
        protected Random random = new Random();

        public TestWaitWindowObserver(long maxChecksBeforeYield, long randomYieldFrequency) {
            this.maxChecksBeforeYield = maxChecksBeforeYield;
            this.randomYieldFrequency = randomYieldFrequency;
        }

        public TestWaitWindowObserver(TestWaitWindowObserver other) {
            this(other.maxChecksBeforeYield, other.randomYieldFrequency);
        }

        private void reset() {
            checksRemainingBeforeYield.set(maxChecksBeforeYield);
            isStarted = true;
        }

        // this only gets called if we have not exceeded maxYields
        @Override
        public void start(String queryId, Range seekRange, long yieldThresholdMs) {
            super.start(queryId, seekRange, Long.MAX_VALUE);
            reset();
        }

        @Override
        public boolean waitWindowOverrun() {
            checksRemainingBeforeYield.decrementAndGet();
            if (isStarted && ((randomYieldFrequency > 0 && (random.nextInt() % randomYieldFrequency == 0)) || checksRemainingBeforeYield.get() <= 0)) {
                checksRemainingBeforeYield.set(0);
                return true;
            } else {
                return false;
            }
        }

        @Override
        public long remainingTimeMs() {
            checksRemainingBeforeYield.decrementAndGet();
            if (isStarted && ((randomYieldFrequency > 0 && (random.nextInt() % randomYieldFrequency == 0)) || checksRemainingBeforeYield.get() <= 0)) {
                checksRemainingBeforeYield.set(0);
                return 0;
            } else {
                return Long.MAX_VALUE;
            }
        }
    }
}
