package datawave.query.iterator.profile;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

import datawave.query.statsd.QueryStatsDClient;

/**
 * Keeps state about the particular session that you are within.
 *
 * Note that spans imply a hierarchy. We don't need that hierarchy. We just want aggregated times.
 *
 */
public class QuerySpan {

    private Logger log = Logger.getLogger(this.getClass());

    protected Collection<QuerySpan> sources;

    protected QueryStatsDClient client;

    protected long sourceCount = 1;

    protected long next = 0;

    protected long seek = 0;

    protected boolean yield = false;

    private Map<String,Long> stageTimers = new LinkedHashMap<>();

    private long stageTimerTotal = 0;

    public enum Stage {
        EmptyTree,
        DocumentSpecificTree,
        FieldIndexTree,
        Aggregation,
        DataTypeAsField,
        DocumentPermutation,
        DocumentEvaluation,
        PostProcessing,
        MaskedValueFilter,
        AttributeKeepFilter,
        DocumentProjection,
        EmptyDocumentFilter,
        KeyAdjudicator,
        DocumentMetadata,
        LimitFields,
        RemoveGroupingContext
    };

    public QuerySpan(QueryStatsDClient client) {
        this.client = client;
        this.sources = Lists.newArrayList();
        this.sourceCount = 1;
    }

    public QuerySpan createSource() {
        QuerySpan newSpan = new QuerySpan(client);
        addSource(newSpan);
        return newSpan;
    }

    public void addSource(QuerySpan sourceSpan) {
        if (client != null) {
            client.addSource();
        }
        sources.add(sourceSpan);
    }

    public long getSourceCount() {
        long sourceCount = this.sourceCount;
        for (QuerySpan subSpan : sources) {
            sourceCount += subSpan.getSourceCount();
        }
        return sourceCount;
    }

    public long getNextCount() {
        long nextCount = next;
        for (QuerySpan subSpan : sources) {
            nextCount += subSpan.getNextCount();
        }
        return nextCount;
    }

    public long getSeekCount() {
        long seekCount = seek;
        for (QuerySpan subSpan : sources) {
            seekCount += subSpan.getSeekCount();
        }
        return seekCount;
    }

    public boolean getYield() {
        if (yield) {
            return true;
        }
        for (QuerySpan subSpan : sources) {
            if (subSpan.getYield()) {
                return true;
            }
        }
        return false;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(super.toString()).append(" sources:").append(getSourceCount()).append(" next:").append(getNextCount()).append(" seek:").append(getSeekCount())
                        .append(" yield:").append(getYield());
        return sb.toString();
    }

    public void logStack(String prefix) {
        StackTraceElement[] stack = Thread.currentThread().getStackTrace();
        StringBuilder sb = new StringBuilder();
        sb.append(prefix).append(this).append("\n");
        for (int x = 1; x < (stack.length - 1); x++) {
            StackTraceElement element = stack[x];
            sb.append(element).append("\n");
        }
        log.trace(sb.toString());
    }

    public synchronized void next() {
        next++;
        if (client != null) {
            client.next();
        }
        if (log.isTraceEnabled()) {
            logStack("next()");
        }
    }

    public synchronized void seek() {
        seek++;
        if (client != null) {
            client.seek();
        }
        if (log.isTraceEnabled()) {
            logStack("seek()");
        }
    }

    public synchronized void yield() {
        yield = true;
        if (client != null) {
            client.yield();
        }
        if (log.isTraceEnabled()) {
            logStack("yield()");
        }
    }

    public void reset() {
        for (QuerySpan source : sources) {
            source.reset();
        }
        sourceCount = 0;
        next = 0;
        seek = 0;
        yield = false;
        stageTimerTotal = 0;
        stageTimers.clear();
    }

    public void addStageTimer(QuerySpan.Stage stageName, long elapsed) {
        stageTimers.put(stageName.toString(), elapsed);
        stageTimerTotal += elapsed;
        if (client != null) {
            client.timing(stageName.toString(), elapsed);
        }
    }

    public boolean hasEntries() {
        if (this.getSeekCount() > 0 || this.getNextCount() > 0 || this.getYield() || this.getSourceCount() > 0 || !this.stageTimers.isEmpty()) {
            return true;
        } else {
            return false;
        }
    }

    public Long getStageTimer(String stageName) {
        return stageTimers.get(stageName);
    }

    public Map<String,Long> getStageTimers() {
        return stageTimers;
    }

    public long getStageTimerTotal() {
        return stageTimerTotal;
    }

    public void setSeek(long seek) {
        this.seek = seek;
    }

    public void setNext(long next) {
        this.next = next;
    }

    public void setYield(boolean yield) {
        this.yield = yield;
    }

    public void setSourceCount(long sourceCount) {
        this.sourceCount = sourceCount;
    }

    public void setStageTimers(Map<String,Long> stageTimers) {
        this.stageTimers.clear();
        for (Map.Entry<String,Long> entry : stageTimers.entrySet()) {
            addStageTimer(QuerySpan.Stage.valueOf(entry.getKey()), entry.getValue());
        }
    }
}
