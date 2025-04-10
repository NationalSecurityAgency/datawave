package datawave.core.query.logic;

import java.util.Map;

import datawave.microservice.querymetric.RangeCounts;

public interface WritesQuerySubplanMetrics {

    Map<String,RangeCounts> getSubPlans();

    void setSubPlans(Map<String,RangeCounts> subPlans);

    void addSubPlan(String newQuery, RangeCounts ranges);
}
