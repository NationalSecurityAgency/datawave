package datawave.query.tracking;

import com.codahale.metrics.SlidingWindowReservoir;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import datawave.query.attributes.Document;
import datawave.query.iterator.profile.QuerySpan;

import java.util.HashMap;
import java.util.Map;
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
    
    private Map<CallType,Long> startTimeMap = new HashMap<>();
    private Map<CallType,Timer> timerMap = new HashMap<>();
    private Map<CallType,Long> numCallsMap = new HashMap<>();
    private Map<CallType,Boolean> inCallMap = new HashMap<>();
    
    public ActiveQuery(String queryId, int windowSize) {
        this.queryId = queryId;
        this.windowSize = windowSize;
        this.initTs = System.currentTimeMillis();
    }
    
    public long getNumCalls(CallType type) {
        Long val = this.numCallsMap.get(type);
        if (val == null) {
            val = 0l;
            this.numCallsMap.put(type, val);
        }
        return val;
    }
    
    public void beginCall(CallType type) {
        this.startTimeMap.put(type, System.currentTimeMillis());
        setInCall(type, true);
    }
    
    public void endCall(CallType type) {
        Long val = getNumCalls(type);
        this.numCallsMap.put(type, (val.longValue() + 1));
        
        Long start = this.startTimeMap.get(type);
        if (start != null) {
            getTimer(type).update(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
            this.startTimeMap.remove(type);
            setInCall(type, false);
        }
    }
    
    public void recordStats(Document document, QuerySpan querySpan) {
        this.documentSizeBytes = document.sizeInBytes();
        this.lastQuerySpan = querySpan;
    }
    
    public long totalElapsedTime() {
        return System.currentTimeMillis() - this.initTs;
    }
    
    public boolean isInCall() {
        return this.inCallMap.values().stream().anyMatch(Predicate.isEqual(Boolean.TRUE));
    }
    
    public String getQueryId() {
        return queryId;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[").append(this.queryId).append("] ");
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
    
    private Timer getTimer(CallType type) {
        Timer t = timerMap.get(type);
        if (t == null) {
            t = new Timer(new SlidingWindowReservoir(this.windowSize));
            this.timerMap.put(type, t);
        }
        return t;
    }
    
    private long lastSourceCount() {
        if (this.lastQuerySpan == null) {
            return 0;
        } else {
            return lastQuerySpan.getSourceCount();
        }
    }
    
    private long lastNextCount() {
        if (this.lastQuerySpan == null) {
            return 0;
        } else {
            return lastQuerySpan.getNextCount();
        }
    }
    
    private long lastSeekCount() {
        if (this.lastQuerySpan == null) {
            return 0;
        } else {
            return lastQuerySpan.getSeekCount();
        }
    }
    
    private long currentCallTime() {
        if (this.inCallMap.values().stream().anyMatch(Predicate.isEqual(Boolean.TRUE))) {
            long now = System.currentTimeMillis();
            long startTime = this.startTimeMap.values().stream().mapToLong(v -> v).min().orElse(now);
            return now - startTime;
        } else {
            return 0;
        }
    }
    
    private void setInCall(CallType type, boolean inCall) {
        this.inCallMap.put(type, inCall);
    }
}
