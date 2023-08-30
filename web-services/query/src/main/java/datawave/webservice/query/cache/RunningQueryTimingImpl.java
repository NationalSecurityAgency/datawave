package datawave.webservice.query.cache;

import datawave.webservice.query.runner.RunningQuery.RunningQueryTiming;

public class RunningQueryTimingImpl implements RunningQueryTiming {

    // The max time allowed within a call (e.g. next())
    private long maxCallMs = 60 * 60 * 1000; // the default is 60, can be overridden in call
    // The time after which we start checking the page size velocity
    private long pageSizeShortCircuitCheckTimeMs = 30 * 60 * 1000;
    // The time after which will we prematurely return if we have results.
    private long pageShortCircuitTimeoutMs = 58 * 60 * 1000;
    // The maximum number of times to continue running a long running query after the timeout is reached.
    private int maxLongRunningTimeoutRetries = 3;

    public RunningQueryTimingImpl(QueryExpirationConfiguration conf, int pageTimeout) {
        this(conf.getCallTimeInMS(), conf.getPageSizeShortCircuitCheckTimeInMS(), conf.getPageShortCircuitTimeoutInMS(),
                        conf.getMaxLongRunningTimeoutRetries());

        if (pageTimeout > 0) {
            maxCallMs = pageTimeout * 60 * 1000;
            pageSizeShortCircuitCheckTimeMs = maxCallMs / 2;
            pageShortCircuitTimeoutMs = Math.round(0.97 * maxCallMs);
        }
    }

    public RunningQueryTimingImpl(long maxCallMs, long pageSizeShortCircuitCheckTimeMs, long pageShortCircuitTimeoutMs) {
        this(maxCallMs, pageSizeShortCircuitCheckTimeMs, pageShortCircuitTimeoutMs, 0);
    }

    public RunningQueryTimingImpl(long maxCallMs, long pageSizeShortCircuitCheckTimeMs, long pageShortCircuitTimeoutMs, int maxLongRunningTimeoutRetries) {
        this.maxCallMs = maxCallMs;
        this.pageSizeShortCircuitCheckTimeMs = pageSizeShortCircuitCheckTimeMs;
        this.pageShortCircuitTimeoutMs = pageShortCircuitTimeoutMs;
        if (maxLongRunningTimeoutRetries > 0) {
            this.maxLongRunningTimeoutRetries = maxLongRunningTimeoutRetries;
        }
    }

    public long getMaxCallMs() {
        return maxCallMs;
    }

    public long getPageSizeShortCircuitCheckTimeMs() {
        return pageSizeShortCircuitCheckTimeMs;
    }

    @Override
    public long getPageShortCircuitTimeoutMs() {
        return pageShortCircuitTimeoutMs;
    }

    @Override
    public int getMaxLongRunningTimeoutRetries() {
        return maxLongRunningTimeoutRetries;
    }

    @Override
    public boolean shouldReturnPartialResults(int pageSize, int maxPageSize, long timeInCall) {

        // only return prematurely if we have at least 1 result
        if (pageSize > 0) {

            // if after the page size short circuit check time
            if (timeInCall >= pageSizeShortCircuitCheckTimeMs) {
                float percentTimeComplete = (float) timeInCall / (float) (this.maxCallMs);
                float percentResultsComplete = (float) pageSize / (float) maxPageSize;
                // if the percent results complete is less than the percent time complete, then break out
                if (percentResultsComplete < percentTimeComplete) {
                    return true;
                }
            }

            // if after the page short circuit timeout, then break out
            if (timeInCall >= this.pageShortCircuitTimeoutMs) {
                return true;
            }

        }

        return false;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("maxCallMs=").append(maxCallMs).append(" ; ");
        builder.append("pageSizeShortCircuitCheckTimeMs=").append(pageSizeShortCircuitCheckTimeMs);
        builder.append("pageShortCircuitTimeoutMs=").append(pageShortCircuitTimeoutMs).append(" ; ");
        return builder.toString();
    }
}
