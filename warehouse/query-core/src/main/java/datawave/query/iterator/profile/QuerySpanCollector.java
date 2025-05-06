package datawave.query.iterator.profile;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

public class QuerySpanCollector {
    private AtomicLong seekCount = new AtomicLong();
    private AtomicLong nextCount = new AtomicLong();
    private AtomicBoolean yield = new AtomicBoolean();
    private AtomicLong sourceCount = new AtomicLong();
    private Map<String,Long> stageTimers = new LinkedHashMap<>();
    private Logger log = Logger.getLogger(QuerySpan.class);

    public void addQuerySpan(QuerySpan querySpan) {

        if (querySpan != null) {
            synchronized (this) {
                seekCount.addAndGet(querySpan.getSeekCount());
                nextCount.addAndGet(querySpan.getNextCount());
                // if yield is set in any querySpan that reports to this collector, then yield should be true
                if (querySpan.getYield()) {
                    yield.set(true);
                }
                sourceCount.addAndGet(querySpan.getSourceCount());
                Map<String,Long> timers = querySpan.getStageTimers();
                for (Map.Entry<String,Long> entry : timers.entrySet()) {
                    String k = entry.getKey();
                    if (stageTimers.containsKey(k)) {
                        long value = stageTimers.get(k).longValue();
                        stageTimers.put(k, (value + entry.getValue().longValue()));
                    } else {
                        stageTimers.put(k, entry.getValue());
                    }
                }
            }
            if (log.isTraceEnabled()) {
                log.trace("thread:" + Thread.currentThread().getId() + " collector: " + this + " added querySpan: " + querySpan);
            }
            querySpan.reset();
        }
    }

    /**
     * A snapshot of the current values.
     *
     * @param reset
     *            - return a QuerySpan and reset the values in this object
     * @return a QuerySpan with the current values of this object
     */
    private QuerySpan snapshot(boolean reset) {
        QuerySpan combinedQuerySpan = null;
        if (hasEntries()) {
            synchronized (this) {
                combinedQuerySpan = new QuerySpan(null);
                if (reset) {
                    combinedQuerySpan.setNext(this.nextCount.getAndSet(0));
                    combinedQuerySpan.setSeek(this.seekCount.getAndSet(0));
                    combinedQuerySpan.setYield(this.yield.getAndSet(false));
                    combinedQuerySpan.setSourceCount(this.sourceCount.getAndSet(0));
                    combinedQuerySpan.setStageTimers(this.stageTimers);
                    this.stageTimers.clear();
                } else {
                    combinedQuerySpan.setNext(this.nextCount.get());
                    combinedQuerySpan.setSeek(this.seekCount.get());
                    combinedQuerySpan.setYield(this.yield.get());
                    combinedQuerySpan.setSourceCount(this.sourceCount.get());
                    combinedQuerySpan.setStageTimers(this.stageTimers);
                }
            }
        }
        return combinedQuerySpan;
    }

    public boolean hasEntries() {
        if (this.seekCount.intValue() > 0 || this.nextCount.intValue() > 0 || this.yield.get() || this.sourceCount.intValue() > 0
                        || !this.stageTimers.isEmpty()) {
            return true;
        } else {
            return false;
        }
    }

    public QuerySpan getCombinedQuerySpan(QuerySpan querySpan, boolean reset) {

        QuerySpan combinedQuerySpan;
        synchronized (this) {
            if (querySpan != null) {
                addQuerySpan(querySpan);
            }
            if (log.isTraceEnabled()) {
                logStack();
            }
            combinedQuerySpan = snapshot(reset);
        }
        return combinedQuerySpan;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(super.toString()).append(" seek:").append(seekCount).append(" next:").append(nextCount).append(" yield:").append(yield).append(" sources:")
                        .append(sourceCount);
        return sb.toString();
    }

    private void logStack() {
        StackTraceElement[] stack = Thread.currentThread().getStackTrace();
        StringBuilder sb = new StringBuilder();
        sb.append("thread:").append(Thread.currentThread().getId()).append(" ").append(this).append(" ").append(this).append("\n");
        for (int x = 1; x < (stack.length - 1); x++) {
            StackTraceElement element = stack[x];
            sb.append(element).append("\n");
        }
        if (log.isTraceEnabled()) {
            log.trace(sb.toString());
        }
    }

    public long getSeekCount() {
        return seekCount.longValue();
    }

    public long getNextCount() {
        return nextCount.longValue();
    }

    public boolean getYield() {
        return yield.get();
    }

    public long getSourceCount() {
        return sourceCount.longValue();
    }

    public Map<String,Long> getStageTimers() {
        return Collections.unmodifiableMap(stageTimers);
    }

}
