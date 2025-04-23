package datawave.microservice.querymetric.handler;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.BaseQueryMetric.PageMetric;
import datawave.microservice.querymetric.QueryMetricType;

public class QueryMetricCombiner<T extends BaseQueryMetric> implements Serializable {
    private static final long serialVersionUID = -5388075643256402640L;
    
    private static final Logger log = LoggerFactory.getLogger(QueryMetricCombiner.class);
    
    public T combineMetrics(T updatedQueryMetric, T cachedQueryMetric, QueryMetricType metricType) {
        
        T combinedMetric = updatedQueryMetric;
        // new metrics coming in may be complete or partial updates
        if (cachedQueryMetric != null) {
            // duplicate cachedQueryMetric so that we leave that object unchanged and return a combined metric
            combinedMetric = (T) cachedQueryMetric.duplicate();
            
            boolean inOrderUpdate = true;
            if (updatedQueryMetric.getLastUpdated() != null && cachedQueryMetric.getLastUpdated() != null) {
                inOrderUpdate = updatedQueryMetric.getLastUpdated().after(cachedQueryMetric.getLastUpdated());
            }
            
            // only update once
            if (combinedMetric.getQueryType() == null && updatedQueryMetric.getQueryType() != null) {
                combinedMetric.setQueryType(updatedQueryMetric.getQueryType());
            }
            // only update once
            if (combinedMetric.getUser() == null && updatedQueryMetric.getUser() != null) {
                combinedMetric.setUser(updatedQueryMetric.getUser());
            }
            // only update once
            if (combinedMetric.getUserDN() == null && updatedQueryMetric.getUserDN() != null) {
                combinedMetric.setUserDN(updatedQueryMetric.getUserDN());
            }
            
            // keep the original createDate
            if (cachedQueryMetric.getCreateDate() != null) {
                combinedMetric.setCreateDate(cachedQueryMetric.getCreateDate());
            }
            
            // Do not update queryId -- shouldn't change anyway
            
            // only update once
            if (combinedMetric.getQuery() == null && updatedQueryMetric.getQuery() != null) {
                combinedMetric.setQuery(updatedQueryMetric.getQuery());
            }
            
            // only update once
            if (combinedMetric.getHost() == null && updatedQueryMetric.getHost() != null) {
                combinedMetric.setHost(updatedQueryMetric.getHost());
            }
            
            // Map page numbers to page metrics and update
            Map<Long,PageMetric> storedPagesByPageNumMap = new TreeMap<>();
            Map<String,PageMetric> storedPagesByUuidMap = new TreeMap<>();
            if (combinedMetric.getPageTimes() != null) {
                combinedMetric.getPageTimes().forEach(pm -> {
                    storedPagesByPageNumMap.put(pm.getPageNumber(), pm);
                    if (pm.getPageUuid() != null) {
                        storedPagesByUuidMap.put(pm.getPageUuid(), pm);
                    }
                });
            }
            // combine all of the page metrics from the cached metric and the updated metric
            if (updatedQueryMetric.getPageTimes() != null) {
                long pageNum = getLastPageNumber(combinedMetric) + 1;
                for (PageMetric updatedPage : updatedQueryMetric.getPageTimes()) {
                    PageMetric storedPage = null;
                    if (updatedPage.getPageUuid() != null) {
                        storedPage = storedPagesByUuidMap.get(updatedPage.getPageUuid());
                    }
                    if (metricType.equals(QueryMetricType.DISTRIBUTED)) {
                        if (storedPage != null) {
                            // updatedPage found by pageUuid
                            updatedPage = combinePageMetrics(updatedPage, storedPage);
                            storedPagesByPageNumMap.put(updatedPage.getPageNumber(), updatedPage);
                        } else {
                            // assume that this is the next page in sequence
                            updatedPage.setPageNumber(pageNum);
                            storedPagesByPageNumMap.put(pageNum, updatedPage);
                            pageNum++;
                        }
                    } else {
                        if (storedPage == null) {
                            storedPage = storedPagesByPageNumMap.get(updatedPage.getPageNumber());
                        }
                        if (storedPage != null) {
                            updatedPage = combinePageMetrics(updatedPage, storedPage);
                        }
                        // page metrics are mapped to their page number to prevent duplicates
                        storedPagesByPageNumMap.put(updatedPage.getPageNumber(), updatedPage);
                    }
                }
            }
            combinedMetric.setPageTimes(new ArrayList<>(storedPagesByPageNumMap.values()));
            
            // only update once
            if (combinedMetric.getProxyServers() == null && updatedQueryMetric.getProxyServers() != null) {
                combinedMetric.setProxyServers(updatedQueryMetric.getProxyServers());
            }
            // only update once
            if (combinedMetric.getErrorMessage() == null && updatedQueryMetric.getErrorMessage() != null) {
                combinedMetric.setErrorMessage(updatedQueryMetric.getErrorMessage());
            }
            // only update once
            if (combinedMetric.getErrorCode() == null && updatedQueryMetric.getErrorCode() != null) {
                combinedMetric.setErrorCode(updatedQueryMetric.getErrorCode());
            }
            // use updated lifecycle unless trying to update a final lifecycle with a non-final lifecycle
            // or if updating with a lifecycle that is less than the current
            if ((combinedMetric.isLifecycleFinal() && !updatedQueryMetric.isLifecycleFinal()) == false
                            && updatedQueryMetric.getLifecycle().compareTo(combinedMetric.getLifecycle()) > 0) {
                combinedMetric.setLifecycle(updatedQueryMetric.getLifecycle());
            }
            // only update once
            if (combinedMetric.getQueryAuthorizations() == null && updatedQueryMetric.getQueryAuthorizations() != null) {
                combinedMetric.setQueryAuthorizations(updatedQueryMetric.getQueryAuthorizations());
            }
            // only update once
            if (combinedMetric.getBeginDate() == null && updatedQueryMetric.getBeginDate() != null) {
                combinedMetric.setBeginDate(updatedQueryMetric.getBeginDate());
            }
            // only update once
            if (combinedMetric.getEndDate() == null && updatedQueryMetric.getEndDate() != null) {
                combinedMetric.setEndDate(updatedQueryMetric.getEndDate());
            }
            // only update once
            if (combinedMetric.getPositiveSelectors() == null && updatedQueryMetric.getPositiveSelectors() != null) {
                combinedMetric.setPositiveSelectors(updatedQueryMetric.getPositiveSelectors());
            }
            // only update once
            if (combinedMetric.getNegativeSelectors() == null && updatedQueryMetric.getNegativeSelectors() != null) {
                combinedMetric.setNegativeSelectors(updatedQueryMetric.getNegativeSelectors());
            }
            if (updatedQueryMetric.getLastUpdated() != null) {
                // keep the latest last updated date
                if (combinedMetric.getLastUpdated() == null || (updatedQueryMetric.getLastUpdated().getTime() > combinedMetric.getLastUpdated().getTime())) {
                    combinedMetric.setLastUpdated(updatedQueryMetric.getLastUpdated());
                }
            }
            // only update once
            if (combinedMetric.getColumnVisibility() == null && updatedQueryMetric.getColumnVisibility() != null) {
                combinedMetric.setColumnVisibility(updatedQueryMetric.getColumnVisibility());
            }
            // only update once
            if (combinedMetric.getQueryLogic() == null && updatedQueryMetric.getQueryLogic() != null) {
                combinedMetric.setQueryLogic(updatedQueryMetric.getQueryLogic());
            }
            // only update once
            if (combinedMetric.getQueryName() == null && updatedQueryMetric.getQueryName() != null) {
                combinedMetric.setQueryName(updatedQueryMetric.getQueryName());
            }
            // only update once
            if (combinedMetric.getParameters() == null && updatedQueryMetric.getParameters() != null) {
                combinedMetric.setParameters(updatedQueryMetric.getParameters());
            }
            // if updatedQueryMetric.setupTime is greater than combinedMetric.setupTime then update
            if (updatedQueryMetric.getSetupTime() > combinedMetric.getSetupTime()) {
                combinedMetric.setSetupTime(updatedQueryMetric.getSetupTime());
            }
            // if updatedQueryMetric.createCallTime is greater than combinedMetric.createCallTime then update
            if (updatedQueryMetric.getCreateCallTime() > combinedMetric.getCreateCallTime()) {
                combinedMetric.setCreateCallTime(updatedQueryMetric.getCreateCallTime());
            }
            // if updatedQueryMetric.loginTime is greater than combinedMetric.loginTime then update
            if (updatedQueryMetric.getLoginTime() > combinedMetric.getLoginTime()) {
                combinedMetric.setLoginTime(updatedQueryMetric.getLoginTime());
            }
            
            if (metricType.equals(QueryMetricType.DISTRIBUTED)) {
                combinedMetric.setSourceCount(combinedMetric.getSourceCount() + updatedQueryMetric.getSourceCount());
                combinedMetric.setNextCount(combinedMetric.getNextCount() + updatedQueryMetric.getNextCount());
                combinedMetric.setSeekCount(combinedMetric.getSeekCount() + updatedQueryMetric.getSeekCount());
                combinedMetric.setYieldCount(combinedMetric.getYieldCount() + updatedQueryMetric.getYieldCount());
                combinedMetric.setDocSize(combinedMetric.getDocSize() + updatedQueryMetric.getDocSize());
                combinedMetric.setDocRanges(combinedMetric.getDocRanges() + updatedQueryMetric.getDocRanges());
                combinedMetric.setFiRanges(combinedMetric.getFiRanges() + updatedQueryMetric.getFiRanges());
            } else {
                combinedMetric.setSourceCount(updatedQueryMetric.getSourceCount());
                combinedMetric.setNextCount(updatedQueryMetric.getNextCount());
                combinedMetric.setSeekCount(updatedQueryMetric.getSeekCount());
                combinedMetric.setYieldCount(updatedQueryMetric.getYieldCount());
                combinedMetric.setDocSize(updatedQueryMetric.getDocSize());
                combinedMetric.setDocRanges(updatedQueryMetric.getDocRanges());
                combinedMetric.setFiRanges(updatedQueryMetric.getFiRanges());
            }
            // update if the update is in-order and the value changed
            if (inOrderUpdate && isChanged(updatedQueryMetric.getPlan(), combinedMetric.getPlan())) {
                combinedMetric.setPlan(updatedQueryMetric.getPlan());
            }
            // only update once
            if (combinedMetric.getPredictions() == null && updatedQueryMetric.getPredictions() != null) {
                combinedMetric.setPredictions(updatedQueryMetric.getPredictions());
            }
            // use the max numUpdates
            combinedMetric.setNumUpdates(Math.max(combinedMetric.getNumUpdates(), updatedQueryMetric.getNumUpdates()));
        }
        log.trace("Combined metrics cached: " + cachedQueryMetric + " updated: " + updatedQueryMetric + " combined: " + combinedMetric);
        return combinedMetric;
    }
    
    public long getLastPageNumber(BaseQueryMetric m) {
        long lastPage = 0;
        List<PageMetric> pageMetrics = m.getPageTimes();
        for (PageMetric pm : pageMetrics) {
            if (lastPage == 0 || pm.getPageNumber() > lastPage) {
                lastPage = pm.getPageNumber();
            }
        }
        return lastPage;
    }
    
    protected PageMetric combinePageMetrics(PageMetric updated, PageMetric stored) {
        if (stored == null) {
            return updated;
        }
        String updatedUuid = updated.getPageUuid();
        String storedUuid = stored.getPageUuid();
        if (updatedUuid != null && storedUuid != null && !updatedUuid.equals(storedUuid)) {
            throw new IllegalStateException(
                            "can not combine page metrics with different pageUuids: " + "updated:" + updated.getPageUuid() + " stored:" + stored.getPageUuid());
        }
        PageMetric pm = new PageMetric(stored);
        if (pm.getHost() == null) {
            pm.setHost(updated.getHost());
        }
        if (pm.getPagesize() == 0) {
            pm.setPagesize(updated.getPagesize());
        }
        if (pm.getReturnTime() == -1) {
            pm.setReturnTime(updated.getReturnTime());
        }
        if (pm.getCallTime() == -1) {
            pm.setCallTime(updated.getCallTime());
        }
        if (pm.getSerializationTime() == -1) {
            pm.setSerializationTime(updated.getSerializationTime());
        }
        if (pm.getBytesWritten() == -1) {
            pm.setBytesWritten(updated.getBytesWritten());
        }
        if (pm.getPageRequested() == 0) {
            pm.setPageRequested(updated.getPageRequested());
        }
        if (pm.getPageReturned() == 0) {
            pm.setPageReturned(updated.getPageReturned());
        }
        if (pm.getLoginTime() == -1) {
            pm.setLoginTime(updated.getLoginTime());
        }
        return pm;
    }
    
    protected boolean isChanged(String updated, String stored) {
        if ((StringUtils.isBlank(stored) && StringUtils.isNotBlank(updated)) || (stored != null && updated != null && !stored.equals(updated))) {
            return true;
        } else {
            return false;
        }
    }
}
