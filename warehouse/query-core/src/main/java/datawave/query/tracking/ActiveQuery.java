package datawave.query.tracking;

import com.codahale.metrics.SlidingWindowReservoir;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import datawave.query.attributes.Document;
import datawave.query.iterator.profile.QuerySpan;
import org.apache.accumulo.core.data.Range;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

public class ActiveQuery {
    
    private String queryId = null;
    private long initTs = 0;
    private QuerySpan lastQuerySpan = null;
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
    
    public ActiveQuery(String queryId, int windowSize) {
        this.queryId = queryId;
        this.windowSize = windowSize;
        this.initTs = System.currentTimeMillis();
    }
    
    synchronized public long getNumCalls(CallType type) {
        Long val = this.numCallsMap.get(type);
        if (val == null) {
            val = 0l;
            this.numCallsMap.put(type, val);
        }
        return val;
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
    
    public void recordStats(Document document, QuerySpan querySpan) {
        this.documentSizeBytes = document.sizeInBytes();
        this.lastQuerySpan = querySpan;
    }
    
    synchronized public long totalElapsedTime() {
        return System.currentTimeMillis() - this.initTs;
    }
    
    synchronized public boolean isInCall() {
        int numInCallTotal = 0;
        for (Integer numInCall : this.inCallMap.values()) {
            numInCallTotal += numInCall;
        }
        return numInCallTotal > 0;
    }
    
    public String getQueryId() {
        return queryId;
    }
    
    synchronized public void finishRange(Range range) {
        this.activeRanges.remove(range);
    }
    
    synchronized public int getNumActiveRanges() {
        return this.activeRanges.size();
    }
    
    @Override
    synchronized public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[").append(this.queryId).append("] ");
        sb.append("ranges=").append(getNumActiveRanges()).append(" ");
        if (isInCall()) {
            sb.append("(tot/cur) ").append(totalElapsedTime()).append("/").append(currentCallTime()).append(" ");
        } else {
            sb.append("(tot) ").append(totalElapsedTime()).append(" ");
        }
        sb.append("(call num/max/avg/min) ");
        for (CallType type : CallType.values()) {
            String t = type.toString().toLowerCase();
            Snapshot s = getTimer(type).getSnapshot();
            sb.append(t).append("=");
            sb.append(getNumCalls(type)).append("/").append(s.getMax() / 1000000).append("/").append(Math.round(s.getMean()) / 1000000).append("/")
                            .append(s.getMin() / 1000000).append(" ");
        }
        
        if (this.documentSizeBytes != 0 && this.lastQuerySpan != null) {
            sb.append("(lastDoc bytes/sources/seek/next) ");
            sb.append(this.documentSizeBytes).append("/");
            sb.append(lastSourceCount()).append("/");
            sb.append(lastSeekCount()).append("/");
            sb.append(lastNextCount());
        }
        return sb.toString();
    }
    
    synchronized private Timer getTimer(CallType type) {
        Timer t = timerMap.get(type);
        if (t == null) {
            t = new Timer(new SlidingWindowReservoir(this.windowSize));
            this.timerMap.put(type, t);
        }
        return t;
    }
    
    synchronized private long lastSourceCount() {
        if (this.lastQuerySpan == null) {
            return 0;
        } else {
            return lastQuerySpan.getSourceCount();
        }
    }
    
    synchronized private long lastNextCount() {
        if (this.lastQuerySpan == null) {
            return 0;
        } else {
            return lastQuerySpan.getNextCount();
        }
    }
    
    synchronized private long lastSeekCount() {
        if (this.lastQuerySpan == null) {
            return 0;
        } else {
            return lastQuerySpan.getSeekCount();
        }
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
    
    synchronized private void setInCall(CallType type, boolean inCall) {
        Integer numCalls = this.inCallMap.get(type);
        if (numCalls == null) {
            numCalls = new Integer(0);
        }
        int newNumCalls;
        if (inCall) {
            newNumCalls = numCalls.intValue() + 1;
        } else {
            newNumCalls = numCalls.intValue() - 1;
        }
        if (newNumCalls < 0) {
            newNumCalls = 0;
        }
        this.inCallMap.put(type, newNumCalls);
    }
}
