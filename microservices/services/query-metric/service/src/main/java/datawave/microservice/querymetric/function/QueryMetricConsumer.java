package datawave.microservice.querymetric.function;

import java.util.List;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.Correlator;
import datawave.microservice.querymetric.QueryMetricOperations;
import datawave.microservice.querymetric.QueryMetricOperationsStats;
import datawave.microservice.querymetric.QueryMetricType;
import datawave.microservice.querymetric.QueryMetricUpdate;
import datawave.microservice.querymetric.QueryMetricUpdateHolder;

public class QueryMetricConsumer implements Consumer<QueryMetricUpdate> {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    private QueryMetricOperations queryMetricOperations;
    private Correlator correlator;
    private QueryMetricOperationsStats stats;
    
    public QueryMetricConsumer(QueryMetricOperations queryMetricOperations, Correlator correlator, QueryMetricOperationsStats stats) {
        this.queryMetricOperations = queryMetricOperations;
        this.correlator = correlator;
        this.stats = stats;
    }
    
    @Override
    public void accept(QueryMetricUpdate queryMetricUpdate) {
        try {
            this.stats.queueTimelyMetrics(queryMetricUpdate);
            this.stats.getMeter(QueryMetricOperationsStats.METERS.MESSAGE_RECEIVE).mark();
            if (shouldCorrelate(queryMetricUpdate)) {
                log.debug("adding update for {} to correlator", queryMetricUpdate.getMetric().getQueryId());
                this.correlator.addMetricUpdate(queryMetricUpdate);
            } else {
                log.debug("storing update for {}", queryMetricUpdate.getMetric().getQueryId());
                this.queryMetricOperations.storeMetricUpdate(new QueryMetricUpdateHolder(queryMetricUpdate));
            }
            
            if (this.correlator.isEnabled()) {
                List<QueryMetricUpdate> correlatedUpdates;
                do {
                    correlatedUpdates = this.correlator.getMetricUpdates(QueryMetricOperations.getInProcess());
                    if (correlatedUpdates != null && !correlatedUpdates.isEmpty()) {
                        try {
                            String queryId = correlatedUpdates.get(0).getMetric().getQueryId();
                            QueryMetricType metricType = correlatedUpdates.get(0).getMetricType();
                            QueryMetricUpdateHolder metricUpdate = this.queryMetricOperations.combineMetricUpdates(correlatedUpdates, metricType);
                            log.debug("storing correlated updates for {}", queryId);
                            queryMetricOperations.storeMetricUpdate(metricUpdate);
                        } catch (Exception e) {
                            log.error("exception while combining correlated updates: " + e.getMessage(), e);
                        }
                    }
                } while (correlatedUpdates != null && !correlatedUpdates.isEmpty());
            }
        } catch (Exception e) {
            log.error("Error processing query metric update message: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }
    
    private boolean shouldCorrelate(QueryMetricUpdate update) {
        // add the first update for a metric to get it into the cache
        if ((update.getMetric().getLifecycle().ordinal() <= BaseQueryMetric.Lifecycle.DEFINED.ordinal())) {
            return false;
        }
        if (this.correlator.isEnabled()) {
            return true;
        } else {
            return false;
        }
    }
}
