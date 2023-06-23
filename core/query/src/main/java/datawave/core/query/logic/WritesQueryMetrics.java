package datawave.core.query.logic;

import datawave.microservice.querymetric.BaseQueryMetric;

public interface WritesQueryMetrics {

    void writeQueryMetrics(BaseQueryMetric metric);

    public boolean hasMetrics();

    public long getSourceCount();

    public long getNextCount();

    public long getSeekCount();

    public long getYieldCount();

    public long getDocRanges();

    public long getFiRanges();

    public void resetMetrics();
}
