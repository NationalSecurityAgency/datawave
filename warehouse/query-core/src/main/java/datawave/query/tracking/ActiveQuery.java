package datawave.query.tracking;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.data.Range;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.codahale.metrics.SlidingWindowReservoir;
import com.codahale.metrics.Timer;

import datawave.query.attributes.Document;
import datawave.query.iterator.profile.QuerySpan;

public class ActiveQuery {

    private static Logger log = Logger.getLogger(ActiveQuery.class);

    private final String queryId;
    private final String activeQueryLogName;
    private long initTs = 0;
    private long lastSourceCount = 0;
    private long lastNextCount = 0;
    private long lastSeekCount = 0;
    private long documentSizeBytes = 0;
    private int windowSize = 0;

    public enum CallType {
        SEEK, NEXT
    };

    private Map<CallType,Map<Range,Long>> startTimeMap = new HashMap<>();
    private Map<CallType,Timer> timerMap = new HashMap<>();
    private Map<CallType,Long> numCallsMap = new HashMap<>();
    private Map<CallType,Integer> inCallMap = new HashMap<>();
    private Set<Range> activeRanges = new HashSet<>();

    public ActiveQuery(String queryId, int windowSize, String activeQueryLogName) {
        this.queryId = queryId;
        this.windowSize = windowSize;
        this.activeQueryLogName = StringUtils.isNotBlank(activeQueryLogName) ? activeQueryLogName : "";
        this.initTs = System.currentTimeMillis();
    }

    synchronized public ActiveQuerySnapshot snapshot() {
        return new ActiveQuerySnapshot(this.activeQueryLogName, this.queryId, this.lastSourceCount, this.lastNextCount, this.lastSeekCount,
                        this.documentSizeBytes, this.activeRanges.size(), this.totalElapsedTime(), this.isInCall(), this.currentCallTime(), this.numCallsMap,
                        this.timerMap);
    }

    synchronized public void beginCall(Range range, CallType type) {
        Map<Range,Long> rangeMap = this.startTimeMap.get(type);
        if (rangeMap == null) {
            rangeMap = new HashMap<>();
            this.startTimeMap.put(type, rangeMap);
        }
        rangeMap.put(range, System.currentTimeMillis());
        activeRanges.add(range);
        setInCall(type, true);
    }

    synchronized public void endCall(Range range, CallType type) {
        Long val = getNumCalls(type);
        this.numCallsMap.put(type, (val.longValue() + 1));

        Map<Range,Long> rangeMap = this.startTimeMap.get(type);
        if (rangeMap != null) {
            Long start = rangeMap.get(range);
            if (start != null) {
                getTimer(type).update(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
                rangeMap.remove(range);
                setInCall(type, false);
            }
        }
    }

    synchronized public void recordStats(Document document, QuerySpan querySpan) {
        if (document != null) {
            this.documentSizeBytes = document.sizeInBytes();
        }
        if (querySpan != null) {
            this.lastSourceCount = querySpan.getSourceCount();
            this.lastSeekCount = querySpan.getSeekCount();
            this.lastNextCount = querySpan.getNextCount();
        }
    }

    synchronized public int removeRange(Range range) {
        this.activeRanges.remove(range);
        return this.activeRanges.size();
    }

    @Override
    synchronized public String toString() {
        return snapshot().toString();
    }

    synchronized private long getNumCalls(CallType type) {
        Long val = this.numCallsMap.get(type);
        if (val == null) {
            val = 0l;
            this.numCallsMap.put(type, val);
        }
        return val;
    }

    synchronized private long totalElapsedTime() {
        return System.currentTimeMillis() - this.initTs;
    }

    synchronized private boolean isInCall() {
        int numInCallTotal = 0;
        for (Integer numInCall : this.inCallMap.values()) {
            numInCallTotal += numInCall;
        }
        return numInCallTotal > 0;
    }

    synchronized private Timer getTimer(CallType type) {
        Timer t = timerMap.get(type);
        if (t == null) {
            t = new Timer(new SlidingWindowReservoir(this.windowSize));
            this.timerMap.put(type, t);
        }
        return t;
    }

    synchronized private void setInCall(CallType type, boolean inCall) {
        Integer numCalls = this.inCallMap.get(type);
        if (numCalls == null) {
            numCalls = 0;
        }
        int newNumCalls;
        if (inCall) {
            newNumCalls = numCalls + 1;
        } else {
            newNumCalls = numCalls - 1;
        }
        if (newNumCalls < 0) {
            log.warn(activeQueryLogName + ": inCall count for callType:" + type.toString() + "=" + newNumCalls + ", resetting to 0");
            newNumCalls = 0;
        }
        this.inCallMap.put(type, newNumCalls);
    }

    synchronized private long currentCallTime() {
        if (isInCall()) {
            long earliestStart = Long.MAX_VALUE;
            for (Map<Range,Long> m : this.startTimeMap.values()) {
                for (Long start : m.values()) {
                    if (start < earliestStart) {
                        earliestStart = start;
                    }
                }
            }
            return System.currentTimeMillis() - earliestStart;
        } else {
            return 0;
        }
    }
}
