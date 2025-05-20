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

    private ThreadLocal<QuerySpan> threadLocalQuerySpan = ThreadLocal.withInitial(() -> new QuerySpan(client));

    private QuerySpanCollector querySpanCollector;

    public MultiThreadedQuerySpan(QuerySpanCollector querySpanCollector, QueryStatsDClient client) {
        super(client);
        this.querySpanCollector = querySpanCollector;
    }

    @Override
    public QuerySpan createSource() {
        QuerySpan querySpan = threadLocalQuerySpan.get();
        try {
            QuerySpan newSpan = new MultiThreadedQuerySpan(querySpanCollector, client);
            querySpan.addSource(newSpan);
            return newSpan;
        } finally {
            if (querySpanCollector != null) {
                querySpanCollector.addQuerySpan(querySpan);
            }
        }
    }

    @Override
    public long getSourceCount() {
        return threadLocalQuerySpan.get().getSourceCount();
    }

    public long getNextCount() {
        return threadLocalQuerySpan.get().getNextCount();
    }

    public long getSeekCount() {
        return threadLocalQuerySpan.get().getSeekCount();
    }

    public boolean getYield() {
        return threadLocalQuerySpan.get().getYield();
    }

    @Override
    public synchronized void next() {
        QuerySpan querySpan = threadLocalQuerySpan.get();
        try {
            querySpan.next();
        } finally {
            if (querySpanCollector != null) {
                querySpanCollector.addQuerySpan(querySpan);
            }
        }
    }

    @Override
    public synchronized void seek() {
        QuerySpan querySpan = threadLocalQuerySpan.get();
        try {
            querySpan.seek();
        } finally {
            if (querySpanCollector != null) {
                querySpanCollector.addQuerySpan(querySpan);
            }
        }
    }

    @Override
    public synchronized void yield() {
        QuerySpan querySpan = threadLocalQuerySpan.get();
        try {
            querySpan.yield();
        } finally {
            if (querySpanCollector != null) {
                querySpanCollector.addQuerySpan(querySpan);
            }
        }
    }

    @Override
    public void reset() {
        threadLocalQuerySpan.get().reset();
    }

    @Override
    public void addStageTimer(Stage stageName, long elapsed) {
        QuerySpan querySpan = threadLocalQuerySpan.get();
        try {
            querySpan.addStageTimer(stageName, elapsed);
        } finally {
            if (querySpanCollector != null) {
                querySpanCollector.addQuerySpan(querySpan);
            }
        }
    }

    @Override
    public Long getStageTimer(String stageName) {
        return threadLocalQuerySpan.get().getStageTimer(stageName);
    }

    @Override
    public Map<String,Long> getStageTimers() {
        return threadLocalQuerySpan.get().getStageTimers();
    }

    @Override
    public long getStageTimerTotal() {
        return threadLocalQuerySpan.get().getStageTimerTotal();
    }

    @Override
    public void setSeek(long seek) {
        QuerySpan querySpan = threadLocalQuerySpan.get();
        try {
            querySpan.setSeek(seek);
        } finally {
            if (querySpanCollector != null) {
                querySpanCollector.addQuerySpan(querySpan);
            }
        }
    }

    @Override
    public void setNext(long next) {
        QuerySpan querySpan = threadLocalQuerySpan.get();
        try {
            querySpan.setNext(next);
        } finally {
            if (querySpanCollector != null) {
                querySpanCollector.addQuerySpan(querySpan);
            }
        }
    }

    @Override
    public void setYield(boolean yield) {
        QuerySpan querySpan = threadLocalQuerySpan.get();
        try {
            querySpan.setYield(yield);
        } finally {
            if (querySpanCollector != null) {
                querySpanCollector.addQuerySpan(querySpan);
            }
        }
    }

    @Override
    public void setSourceCount(long sourceCount) {
        QuerySpan querySpan = threadLocalQuerySpan.get();
        try {
            querySpan.setSourceCount(sourceCount);
        } finally {
            if (querySpanCollector != null) {
                querySpanCollector.addQuerySpan(querySpan);
            }
        }
    }

    @Override
    public void setStageTimers(Map<String,Long> stageTimers) {
        QuerySpan querySpan = threadLocalQuerySpan.get();
        try {
            querySpan.setStageTimers(stageTimers);
        } finally {
            if (querySpanCollector != null) {
                querySpanCollector.addQuerySpan(querySpan);
            }
        }
    }

    public String toString() {
        return threadLocalQuerySpan.get().toString();
    }
}
