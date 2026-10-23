package datawave.webservice.query.runner;

import datawave.core.query.logic.QueryLogic;
import datawave.microservice.query.Query;
import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.webservice.query.runner.RunningQuery.RunningQueryTiming;

/**
 * Page limits are normalized once and checked before another iterator read. A full page is not a terminal query outcome.
 */
final class PagePolicy {
    private final int pageSize;
    private final long resultLimit;
    private final long workLimit;
    private final long byteLimit;
    private final RunningQueryTiming timing;
    private final long startNanos = System.nanoTime();

    PagePolicy(Query settings, QueryLogic<?> logic, long maxResults, RunningQueryTiming timing) {
        pageSize = logic.getMaxPageSize() > 0 ? Math.min(settings.getPagesize(), logic.getMaxPageSize()) : settings.getPagesize();
        resultLimit = settings.isMaxResultsOverridden() ? settings.getMaxResultsOverride() : maxResults;
        workLimit = logic.getMaxWork();
        byteLimit = logic.getPageByteTrigger();
        this.timing = timing;
    }

    PageStopReason beforeRead(int count, long bytes, long totalResults, BaseQueryMetric metric) {
        if (resultLimit >= 0 && totalResults >= resultLimit) {
            return PageStopReason.RESULT_LIMIT;
        }
        if (workLimit >= 0 && (metric.getNextCount() >= workLimit || metric.getSeekCount() >= workLimit - metric.getNextCount())) {
            return PageStopReason.WORK_LIMIT;
        }
        if (count >= pageSize) {
            return PageStopReason.FULL_PAGE;
        }
        if (byteLimit > 0 && bytes >= byteLimit) {
            return PageStopReason.BYTE_LIMIT;
        }
        long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000;
        if (timing != null && count > 0 && timing.shouldReturnPartialResults(count, pageSize, elapsedMillis)) {
            return PageStopReason.DEADLINE;
        }
        return null;
    }

    boolean countBytes() {
        return byteLimit > 0;
    }
}
