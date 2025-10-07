package datawave.webservice.query.hud;

/**
 *
 */
public class HudMetricSummary {
    private long hours = 0L;
    private long queryCount = 0L;
    private long minPageResponseTime = 0L;
    private long totalPageResponseTime = 0L;
    private long totalPages = 0L;
    private long maxPageResponseTime = 0L;
    private long minPageResultSize = 0L;
    private long totalPageResultSize = 0L;
    private long maxPageResultSize = 0;

    /**
     * @return the hours
     */
    public long getHours() {
        return hours;
    }

    /**
     * @param hours
     *            the hours to set
     */
    public void setHours(long hours) {
        this.hours = hours;
    }

    /**
     * @return the queryCount
     */
    public long getQueryCount() {
        return queryCount;
    }

    /**
     * @param queryCount
     *            the queryCount to set
     */
    public void setQueryCount(long queryCount) {
        this.queryCount = queryCount;
    }

    /**
     * @return the minPageResponseTime
     */
    public long getMinPageResponseTime() {
        return minPageResponseTime;
    }

    /**
     * @param minPageResponseTime
     *            the minPageResponseTime to set
     */
    public void setMinPageResponseTime(long minPageResponseTime) {
        this.minPageResponseTime = minPageResponseTime;
    }

    /**
     * @return the totalPageResponseTime
     */
    public long getTotalPageResponseTime() {
        return totalPageResponseTime;
    }

    /**
     * @param totalPageResponseTime
     *            the totalPageResponseTime to set
     */
    public void setTotalPageResponseTime(long totalPageResponseTime) {
        this.totalPageResponseTime = totalPageResponseTime;
    }

    /**
     * @return the totalPages
     */
    public long getTotalPages() {
        return totalPages;
    }

    /**
     * @param totalPages
     *            the totalPages to set
     */
    public void setTotalPages(long totalPages) {
        this.totalPages = totalPages;
    }

    /**
     * @return the maxPageResponseTime
     */
    public long getMaxPageResponseTime() {
        return maxPageResponseTime;
    }

    /**
     * @param maxPageResponseTime
     *            the maxPageResponseTime to set
     */
    public void setMaxPageResponseTime(long maxPageResponseTime) {
        this.maxPageResponseTime = maxPageResponseTime;
    }

    /**
     * @return the minPageResultSize
     */
    public long getMinPageResultSize() {
        return minPageResultSize;
    }

    /**
     * @param minPageResultSize
     *            the minPageResultSize to set
     */
    public void setMinPageResultSize(long minPageResultSize) {
        this.minPageResultSize = minPageResultSize;
    }

    /**
     * @return the totalPageResultSize
     */
    public long getTotalPageResultSize() {
        return totalPageResultSize;
    }

    /**
     * @param totalPageResultSize
     *            the totalPageResultSize to set
     */
    public void setTotalPageResultSize(long totalPageResultSize) {
        this.totalPageResultSize = totalPageResultSize;
    }

    /**
     * @return the maxPageResultSize
     */
    public long getMaxPageResultSize() {
        return maxPageResultSize;
    }

    /**
     * @param maxPageResultSize
     *            the maxPageResultSize to set
     */
    public void setMaxPageResultSize(long maxPageResultSize) {
        this.maxPageResultSize = maxPageResultSize;
    }

}
