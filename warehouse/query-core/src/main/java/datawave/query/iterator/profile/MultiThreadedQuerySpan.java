package datawave.query.iterator.profile;

import java.util.Map;

import datawave.query.statsd.QueryStatsDClient;

/**
 * Keeps state about the particular session that you are within.
 *
 * Note that spans imply a hierarchy. We don't need that hierarchy. We just want aggregated times.
 *
 */
public class MultiThreadedQuerySpan extends QuerySpan {

    private ThreadLocal<QuerySpan> threadLocalQuerySpan = new ThreadLocal<>();

    public QuerySpan getThreadSpecificQuerySpan() {
        QuerySpan querySpan = threadLocalQuerySpan.get();
        if (querySpan == null) {
            querySpan = new QuerySpan(client);
            threadLocalQuerySpan.set(querySpan);
        }
        return querySpan;
    }

    public MultiThreadedQuerySpan(QueryStatsDClient client) {
        super(client);
    }

    @Override
    public QuerySpan createSource() {
        QuerySpan newSpan = new MultiThreadedQuerySpan(client);
        getThreadSpecificQuerySpan().addSource(newSpan);
        return newSpan;
    }

    @Override
    public long getSourceCount() {
        return getThreadSpecificQuerySpan().getSourceCount();
    }

    public long getNextCount() {
        return getThreadSpecificQuerySpan().getNextCount();
    }

    public long getSeekCount() {
        return getThreadSpecificQuerySpan().getSeekCount();
    }

    public boolean getYield() {
        return getThreadSpecificQuerySpan().getYield();
    }

    @Override
    public synchronized void next() {
        getThreadSpecificQuerySpan().next();
    }

    @Override
    public synchronized void seek() {
        getThreadSpecificQuerySpan().seek();
    }

    @Override
    public synchronized void yield() {
        getThreadSpecificQuerySpan().yield();
    }

    @Override
    public void reset() {
        super.reset();
        getThreadSpecificQuerySpan().reset();
    }

    @Override
    public void addStageTimer(Stage stageName, long elapsed) {
        getThreadSpecificQuerySpan().addStageTimer(stageName, elapsed);
    }

    @Override
    public Long getStageTimer(String stageName) {
        return getThreadSpecificQuerySpan().getStageTimer(stageName);
    }

    @Override
    public Map<String,Long> getStageTimers() {
        return getThreadSpecificQuerySpan().getStageTimers();
    }

    @Override
    public long getStageTimerTotal() {
        return getThreadSpecificQuerySpan().getStageTimerTotal();
    }

    @Override
    public void setSeek(long seek) {
        getThreadSpecificQuerySpan().setSeek(seek);
    }

    @Override
    public void setNext(long next) {
        getThreadSpecificQuerySpan().setNext(next);
    }

    @Override
    public void setYield(boolean yield) {
        getThreadSpecificQuerySpan().setYield(yield);
    }

    @Override
    public void setSourceCount(long sourceCount) {
        getThreadSpecificQuerySpan().setSourceCount(sourceCount);
    }

    @Override
    public void setStageTimers(Map<String,Long> stageTimers) {
        getThreadSpecificQuerySpan().setStageTimers(stageTimers);
    }

    public String toString() {
        return getThreadSpecificQuerySpan().toString();
    }
}
