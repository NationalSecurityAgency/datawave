package datawave.webservice.query.metric;

import datawave.configuration.DatawaveEmbeddedProjectStageHolder;

import datawave.metrics.remote.RemoteQueryMetricService;

import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.webservice.query.exception.QueryExceptionType;
import datawave.webservice.result.VoidResponse;
import org.apache.deltaspike.core.api.exclude.Exclude;
import org.apache.log4j.Logger;

import javax.annotation.Resource;
import javax.annotation.security.DeclareRoles;
import javax.annotation.security.RolesAllowed;
import javax.annotation.security.RunAs;
import javax.ejb.LocalBean;
import javax.ejb.Lock;
import javax.ejb.LockType;
import javax.ejb.Schedule;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.inject.Inject;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

@RunAs("InternalUser")
@RolesAllowed({"AuthorizedUser", "AuthorizedQueryServer", "InternalUser", "Administrator"})
@DeclareRoles({"AuthorizedUser", "AuthorizedQueryServer", "InternalUser", "Administrator"})
@Startup
@Singleton
@LocalBean
@Lock(LockType.READ)
@Exclude(ifProjectStage = DatawaveEmbeddedProjectStageHolder.DatawaveEmbedded.class)
public class QueryMetricsWriter {
    
    private Logger log = Logger.getLogger(this.getClass());
    
    @Inject
    private JMSContext jmsContext;
    
    @Resource(mappedName = "java:/queue/QueryMetrics")
    private Queue dest;
    
    @Inject
    private RemoteQueryMetricService remoteQueryMetricService;
    
    private List<FailureRecord> failedMetricList = new ArrayList<>();
    
    private static volatile AtomicBoolean receivingMetrics = new AtomicBoolean(false);
    
    private List<QueryMetricHolder> getMetricsFromQueue() {
        List metricHolderList = new ArrayList<>();
        long start = System.currentTimeMillis();
        try (JMSConsumer consumer = jmsContext.createConsumer(dest)) {
            Message message;
            do {
                message = consumer.receive(500);
                if (message == null) {
                    log.error("received a null message");
                } else {
                    try {
                        if (message instanceof ObjectMessage) {
                            ObjectMessage objectMessage = (ObjectMessage) message;
                            Object o = objectMessage.getObject();
                            QueryMetricHolder queryMetricHolder = null;
                            if (o instanceof QueryMetricHolder) {
                                queryMetricHolder = (QueryMetricHolder) o;
                            } else if (o instanceof QueryMetricMessage) {
                                queryMetricHolder = ((QueryMetricMessage) o).getMetricHolder();
                            } else {
                                log.error("message of type " + message.getClass().getCanonicalName() + " not expected");
                            }
                            if (queryMetricHolder != null) {
                                metricHolderList.add(queryMetricHolder);
                            }
                        }
                    } catch (Exception e) {
                        log.error(e.getMessage() + " messageID:" + message.getJMSMessageID());
                        continue;
                    }
                }
                // break out of loop every minute to ensure flush and acknowledge messages
                if (metricHolderList.size() >= 1000 || (System.currentTimeMillis() - start) > 60000) {
                    break;
                }
            } while (message != null);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return metricHolderList;
    }
    
    private static class FailureRecord {
        private BaseQueryMetric metric;
        private long created = System.currentTimeMillis();
        private int totalFailures;
        private int failuresWhenOthersSucceeded;
        
        public FailureRecord(BaseQueryMetric metric, boolean anySuccess) {
            this.metric = metric;
            this.totalFailures = 1;
            this.failuresWhenOthersSucceeded = anySuccess ? 1 : 0;
        }
        
        public BaseQueryMetric getMetric() {
            return metric;
        }
        
        public void incrementFailures(boolean anySuccess) {
            totalFailures++;
            if (anySuccess) {
                failuresWhenOthersSucceeded++;
            }
        }
        
        public int getTotalFailures() {
            return totalFailures;
        }
        
        public int getFailuresWhenOthersSucceeded() {
            return failuresWhenOthersSucceeded;
        }
        
        public long getAge() {
            return System.currentTimeMillis() - created;
        }
    }
    
    @Schedule(hour = "*", minute = "*", second = "*/10", persistent = false)
    public void processQueryMetrics() {
        if (receivingMetrics.compareAndSet(false, true)) {
            try {
                List<BaseQueryMetric> metricQueue = getMetricsFromQueue().stream().map(h -> h.getQueryMetric()).collect(Collectors.toList());
                if (!metricQueue.isEmpty()) {
                    try {
                        writeMetricsToRemoteService(metricQueue);
                    } catch (Exception e) {
                        log.error(metricQueue.size() + " metrics failed write to remote service as a batch, will retry individually - " + e.getMessage(), e);
                        List<BaseQueryMetric> failedMetrics = writeMetricsToRemoteServiceIndividually(metricQueue);
                        boolean anySuccess = failedMetrics.size() < metricQueue.size();
                        failedMetrics.forEach(m -> {
                            failedMetricList.add(new FailureRecord(m, anySuccess));
                        });
                    }
                    retryFailures();
                }
            } finally {
                receivingMetrics.set(false);
            }
        }
    }
    
    private void retryFailures() {
        Iterator<FailureRecord> itr = failedMetricList.iterator();
        boolean anySuccess = false;
        while (itr.hasNext()) {
            FailureRecord f = itr.next();
            if (f.getFailuresWhenOthersSucceeded() > 2) {
                log.error("discarding metric: " + f.getMetric());
                itr.remove();
            } else {
                try {
                    writeMetricsToRemoteService(Collections.singletonList(f.getMetric()));
                    itr.remove();
                    anySuccess = true;
                } catch (Exception e1) {
                    log.error("metric failed write to remote service, will retry - " + e1.getMessage());
                }
            }
        }
        for (FailureRecord f : failedMetricList) {
            f.incrementFailures(anySuccess);
        }
    }
    
    private List<BaseQueryMetric> writeMetricsToRemoteServiceIndividually(List<BaseQueryMetric> metricQueue) {
        List<BaseQueryMetric> failedMetrics = new ArrayList<>();
        metricQueue.forEach(m -> {
            try {
                writeMetricsToRemoteService(Collections.singletonList(m));
            } catch (Exception e1) {
                log.error("metric failed write to remote service, will retry - " + e1.getMessage());
                failedMetrics.add(m);
            }
        });
        return failedMetrics;
    }
    
    private void writeMetricsToRemoteService(List<BaseQueryMetric> updatedMetrics) throws Exception {
        if (!updatedMetrics.isEmpty()) {
            VoidResponse response = remoteQueryMetricService.updateMetrics(updatedMetrics);
            List<QueryExceptionType> exceptions = response.getExceptions();
            if (exceptions != null && !exceptions.isEmpty()) {
                throw new RuntimeException(exceptions.get(0).getMessage());
            }
        }
    }
}
