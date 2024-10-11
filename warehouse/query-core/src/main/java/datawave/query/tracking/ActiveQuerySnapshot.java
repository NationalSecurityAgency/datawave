package datawave.query.tracking;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;

public class ActiveQuerySnapshot {

    public static final Comparator<ActiveQuerySnapshot> greatestElapsedTime = Comparator.comparingLong(ActiveQuerySnapshot::totalElapsedTime).reversed();

    private final String activeQueryLogName;
    private final String queryId;
    private final long lastSourceCount;
    private final long lastNextCount;
    private final long lastSeekCount;
    private final long lastEvaluatedCount;
    private final long lastRejectedCount;
    private final long documentSizeBytes;

    private final long totalElapsedTime;
    private final long currentCallTime;
    private final boolean isInCall;
    private final int numActiveRanges;

    private final Map<ActiveQuery.CallType,Long> numCallsMap = new HashMap<>();
    private final Map<ActiveQuery.CallType,Snapshot> snapshotMap = new HashMap<>();

    public ActiveQuerySnapshot(String activeQueryLogName, String queryId, long lastSourceCount, long lastNextCount, long lastSeekCount, long lastEvaluatedCount,
                    long lastRejectedCount, long documentSizeBytes, int numActiveRanges, long totalElapsedTime, boolean isInCall, long currentCallTime,
                    Map<ActiveQuery.CallType,Long> numCallsMap, Map<ActiveQuery.CallType,Timer> timerMap) {
        this.activeQueryLogName = activeQueryLogName;
        this.queryId = queryId;
        this.lastSourceCount = lastSourceCount;
        this.lastNextCount = lastNextCount;
        this.lastSeekCount = lastSeekCount;
        this.lastEvaluatedCount = lastEvaluatedCount;
        this.lastRejectedCount = lastRejectedCount;
        this.documentSizeBytes = documentSizeBytes;
        this.numActiveRanges = numActiveRanges;

        this.totalElapsedTime = totalElapsedTime;
        this.currentCallTime = currentCallTime;
        this.isInCall = isInCall;
        for (Map.Entry<ActiveQuery.CallType,Long> entry : numCallsMap.entrySet()) {
            this.numCallsMap.put(entry.getKey(), entry.getValue().longValue());
        }
        for (Map.Entry<ActiveQuery.CallType,Timer> entry : timerMap.entrySet()) {
            this.snapshotMap.put(entry.getKey(), entry.getValue().getSnapshot());
        }
    }

    public long getNumCalls(ActiveQuery.CallType type) {
        if (this.numCallsMap.containsKey(type)) {
            return this.numCallsMap.get(type);
        } else {
            return 0;
        }
    }

    public long totalElapsedTime() {
        return totalElapsedTime;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[").append(activeQueryLogName).append("]");
        sb.append("[").append(this.queryId).append("] ");
        sb.append("ranges=").append(this.numActiveRanges).append(" ");
        if (this.isInCall) {
            sb.append("(tot/cur) ").append(this.totalElapsedTime).append("/").append(this.currentCallTime).append(" ");
        } else {
            sb.append("(tot) ").append(this.totalElapsedTime).append(" ");
        }
        sb.append("(call num/max/avg/min) ");
        for (ActiveQuery.CallType type : ActiveQuery.CallType.values()) {
            String t = type.toString().toLowerCase();
            Snapshot s = this.snapshotMap.get(type);
            if (s != null) {
                sb.append(t).append("=");
                sb.append(getNumCalls(type)).append("/").append(s.getMax() / 1000000).append("/").append(Math.round(s.getMean()) / 1000000).append("/")
                                .append(s.getMin() / 1000000).append(" ");
            }
        }

        if (this.documentSizeBytes > 0) {
            sb.append("(lastDoc bytes/sources/seek/next) ");
            sb.append(this.documentSizeBytes).append("/");
            sb.append(this.lastSourceCount).append("/");
            sb.append(this.lastSeekCount).append("/");
            sb.append(this.lastEvaluatedCount).append("/");
            sb.append(this.lastRejectedCount).append("/");
            sb.append(this.lastNextCount);
        }
        return sb.toString();
    }
}
