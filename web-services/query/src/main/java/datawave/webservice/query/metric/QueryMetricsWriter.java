package datawave.webservice.query.metric;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import javax.annotation.security.RunAs;
import javax.ejb.LocalBean;
import javax.ejb.Lock;
import javax.ejb.LockType;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.enterprise.concurrent.ManagedThreadFactory;
import javax.enterprise.event.Observes;
import javax.enterprise.inject.spi.BeanManager;
import javax.inject.Inject;
import javax.jms.ConnectionFactory;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.Queue;

import org.apache.commons.collections4.map.LRUMap;
import org.apache.commons.lang.StringUtils;
import org.apache.deltaspike.core.api.exclude.Exclude;
import org.apache.log4j.Logger;

import datawave.configuration.DatawaveEmbeddedProjectStageHolder;
import datawave.configuration.RefreshEvent;
import datawave.configuration.spring.SpringBean;
import datawave.core.query.metric.QueryMetricHandler;
import datawave.metrics.remote.RemoteQueryMetricService;
import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.BaseQueryMetric.Lifecycle;
import datawave.microservice.querymetric.BaseQueryMetric.PageMetric;
import datawave.util.timely.UdpClient;
import datawave.webservice.query.exception.QueryExceptionType;
import datawave.webservice.result.VoidResponse;

@RunAs("InternalUser")
@Startup
@Singleton
@LocalBean
@Lock(LockType.READ)
@Exclude(ifProjectStage = DatawaveEmbeddedProjectStageHolder.DatawaveEmbedded.class)
public class QueryMetricsWriter {

    private Logger log = Logger.getLogger(QueryMetricsWriter.class);

    @Resource(mappedName = "java:jboss/ee/concurrency/factory/default")
    private ManagedThreadFactory managedThreadFactory;

    @Resource(mappedName = "java:jboss/DefaultJMSConnectionFactory")
    private ConnectionFactory connectionFactory;

    @Resource(mappedName = "java:/queue/QueryMetrics")
    private Queue dest;

    @Inject
    private QueryMetricHandler<? extends BaseQueryMetric> queryMetricHandler;

    @Inject
    private RemoteQueryMetricService remoteQueryMetricService;

    @Inject
    @SpringBean(name = "QueryMetricsWriterConfiguration", refreshable = true)
    private QueryMetricsWriterConfiguration queryMetricsWriterConfiguration;

    private UdpClient timelyClient = null;
    private Map<String,Long> lastPageMetricMap;

    private DecimalFormat df = new DecimalFormat("0.00");
    private Future future = null;
    private volatile boolean shuttingDown = false;
    private List<FailureRecord> failedMetrics = new ArrayList<>();

    @PostConstruct
    private void init() {
        // noinspection unchecked
        lastPageMetricMap = new LRUMap(1000);
        timelyClient = createUdpClient();
        ExecutorService executorService = Executors.newSingleThreadExecutor(managedThreadFactory);
        this.future = executorService.submit(new MetricProcessor());
    }

    @PreDestroy
    public void shutdown() {
        // try to ensure that the task running on the managed thread exits before shutdown
        this.shuttingDown = true;
        try {
            this.future.get(5000, TimeUnit.SECONDS);
        } catch (Exception e) {

        }
    }

    private UdpClient createUdpClient() {
        if (queryMetricsWriterConfiguration != null && StringUtils.isNotBlank(queryMetricsWriterConfiguration.getTimelyHost())) {
            return new UdpClient(queryMetricsWriterConfiguration.getTimelyHost(), queryMetricsWriterConfiguration.getTimelyPort());
        } else {
            return null;
        }
    }

    public void onRefresh(@Observes RefreshEvent event, BeanManager bm) {
        // protect timelyClient from being used in sendMetricsToTimely while re-creating the client
        synchronized (this) {
            timelyClient = createUdpClient();
        }
    }

    private List<QueryMetricHolder> getMetricsFromQueue(JMSContext jmsContext) {
        List metricHolderList = new ArrayList<>();
        long start = System.currentTimeMillis();
        try (JMSConsumer consumer = jmsContext.createConsumer(dest)) {
            Message message;
            do {
                message = consumer.receive(500);
                if (message != null) {
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
                // break out of loop when batchSize is met or at least every minute
                if (metricHolderList.size() >= queryMetricsWriterConfiguration.getBatchSize() || (System.currentTimeMillis() - start) > 60000) {
                    break;
                }
            } while (message != null);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return metricHolderList;
    }

    private static class FailureRecord {
        private QueryMetricHolder queryMetricHolder;
        private long created = System.currentTimeMillis();
        private long totalFailures;
        private long failuresWhenOthersSucceeded;

        public FailureRecord(BaseQueryMetric metric, boolean anySuccess) {
            this(new QueryMetricHolder(null, metric), anySuccess);
        }

        public FailureRecord(QueryMetricHolder queryMetricHolder, boolean anySuccess) {
            this.queryMetricHolder = queryMetricHolder;
            this.totalFailures = 1;
            this.failuresWhenOthersSucceeded = anySuccess ? 1 : 0;
        }

        public BaseQueryMetric getMetric() {
            return queryMetricHolder.getQueryMetric();
        }

        public QueryMetricHolder getQueryMetricHolder() {
            return queryMetricHolder;
        }

        public void incrementFailures(boolean anySuccess) {
            totalFailures++;
            if (anySuccess) {
                failuresWhenOthersSucceeded++;
            }
        }

        public long getTotalFailures() {
            return totalFailures;
        }

        public long getFailuresWhenOthersSucceeded() {
            return failuresWhenOthersSucceeded;
        }

        public long getAge() {
            return System.currentTimeMillis() - created;
        }
    }

    private class MetricProcessor implements Runnable {
        @Override
        public void run() {
            // create JMSContext inside the ManagedThread so that the
            // session is active when we use it to create a consumer
            JMSContext jmsContext = connectionFactory.createContext(JMSContext.AUTO_ACKNOWLEDGE);
            boolean useRemoteService = queryMetricsWriterConfiguration.getUseRemoteService();
            while (!shuttingDown) {
                try {
                    // only process metrics if they are being successfully written
                    if (failedMetrics.isEmpty()) {
                        List<QueryMetricHolder> metricQueue = getMetricsFromQueue(jmsContext);
                        if (metricQueue.isEmpty()) {
                            Thread.sleep(5000);
                        } else {
                            if (useRemoteService) {
                                processQueryMetricsWithRemoteService(metricQueue);
                            } else {
                                processQueryMetricsWithHandler(metricQueue);
                            }
                        }
                    } else {
                        boolean success = writeFailedMetrics();
                        if (!success) {
                            // prevent a tight loop where we keep failing to write failedMetrics
                            Thread.sleep(60000);
                        }
                    }
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }
        }
    }

    private void processQueryMetricsWithRemoteService(List<QueryMetricHolder> metricHolderQueue) {
        List<BaseQueryMetric> metricQueue = metricHolderQueue.stream().map(QueryMetricHolder::getQueryMetric).collect(Collectors.toList());
        if (!metricQueue.isEmpty()) {
            try {
                writeMetricsToRemoteService(metricQueue);
                log.debug("wrote " + metricQueue.size() + " query metric updates to the RemoteQueryMetricService");
            } catch (Exception e) {
                log.error(metricQueue.size() + " metrics failed write to RemoteQueryMetricService as a batch, will retry individually - " + e.getMessage(), e);
                metricQueue.forEach(m -> {
                    failedMetrics.add(new FailureRecord(m, false));
                });
            }
        }
    }

    private void processQueryMetricsWithHandler(List<QueryMetricHolder> metricQueue) {
        List<QueryMetricHolder> currentFailures = new ArrayList<>();
        AtomicBoolean anySuccess = new AtomicBoolean(false);
        try {
            if (!metricQueue.isEmpty()) {
                currentFailures.addAll(writeMetricsToHandler(queryMetricHandler, metricQueue));
                log.debug("wrote " + (metricQueue.size() - currentFailures.size()) + " query metric updates to queryMetricHandler");
                anySuccess.set(currentFailures.size() < metricQueue.size());
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            currentFailures.addAll(metricQueue);
        } finally {
            if (!metricQueue.isEmpty()) {
                if (!currentFailures.isEmpty()) {
                    log.error(currentFailures.size() + " metrics failed write to queryMetricHandler, will retry individually");
                    currentFailures.stream().forEach(queryMetricHolder -> {
                        this.failedMetrics.add(new FailureRecord(queryMetricHolder, anySuccess.get()));
                    });
                }
                if (!anySuccess.get()) {
                    // error during write or flush, create a new handler so that we can re-try next time
                    log.error("error writing " + metricQueue.size() + " query metric updates - creating new queryMetricHandler");
                    queryMetricHandler.reload();
                }
            }
        }
    }

    private boolean writeFailedMetrics() {
        Iterator<FailureRecord> itr = failedMetrics.iterator();
        int successful = 0;
        while (itr.hasNext()) {
            FailureRecord f = itr.next();
            if (queryMetricsWriterConfiguration.getUseRemoteService()) {
                try {
                    writeMetricsToRemoteService(Collections.singletonList(f.getMetric()));
                    itr.remove();
                    successful++;
                } catch (Exception e) {
                    // failures will remain in failedMetrics and be processed in processFailedMetricList
                }
            } else {
                // On failure, writeMetricsToHandler returns a list of failed metrics. If empty, then success
                if (writeMetricsToHandler(queryMetricHandler, Collections.singletonList(f.getQueryMetricHolder())).isEmpty()) {
                    itr.remove();
                    successful++;
                }
            }
        }
        log.debug("writeFailedMetrics: success:" + successful + " failures:" + failedMetrics.size());
        boolean anySuccessful = successful > 0;
        processFailedMetricList(anySuccessful);
        return failedMetrics.isEmpty() || anySuccessful;
    }

    private void processFailedMetricList(boolean anySuccessful) {
        long discardForFailureCount = 0;
        Set<String> discardForFailureMetrics = new TreeSet<>();
        long discardForTimeCount = 0;
        Set<String> discardForTimeMetrics = new TreeSet<>();
        Iterator<FailureRecord> itr = failedMetrics.iterator();
        while (itr.hasNext()) {
            FailureRecord f = itr.next();
            f.incrementFailures(anySuccessful);
            String queryId = f.getMetric().getQueryId();
            long iFailures = f.getFailuresWhenOthersSucceeded();
            long tFailures = f.getTotalFailures();
            if (iFailures >= 2) {
                // If a metric update fails to write twice when others succeeded, then discard the update
                itr.remove();
                discardForFailureMetrics.add(queryId);
                discardForFailureCount++;
            } else if (f.getAge() > TimeUnit.MINUTES.toMillis(60)) {
                // Don't allow failed metrics to stop metric writing forever. Either there is a system problem or
                // all updates in failedMetrics are coincidentally failing for some metric-specific reason
                itr.remove();
                discardForTimeMetrics.add(queryId);
                discardForTimeCount++;
            } else {
                log.trace("failures individual/total:" + iFailures + "/" + tFailures + " for metric update " + queryId);
            }
        }
        if (discardForFailureCount > 0) {
            log.error("Discarding " + discardForFailureCount + " updates from queries " + discardForFailureMetrics + " for repeated failures");
        }
        if (discardForTimeCount > 0) {
            log.error("Discarding " + discardForTimeCount + " updates from queries " + discardForTimeMetrics + " for exceeding max time in failure queue");
        }
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

    private List<QueryMetricHolder> writeMetricsToHandler(QueryMetricHandler queryMetricHandler, List<QueryMetricHolder> metricQueue) {
        List<QueryMetricHolder> failedMetrics = new ArrayList<>();
        if (!metricQueue.isEmpty()) {
            for (QueryMetricHolder metricHolder : metricQueue) {
                try {
                    queryMetricHandler.updateMetric(metricHolder.getQueryMetric(), metricHolder.getPrincipal());
                    try {
                        sendMetricsToTimely(metricHolder.getQueryMetric());
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }
                } catch (Exception e) {
                    log.error("query metric update failed: " + e.getMessage());
                    failedMetrics.add(metricHolder);
                }
            }
        }
        return failedMetrics;
    }

    private synchronized void sendMetricsToTimely(BaseQueryMetric queryMetric) {

        if (timelyClient != null && queryMetric.getQueryType().equalsIgnoreCase("RunningQuery")) {
            try {
                String queryId = queryMetric.getQueryId();
                BaseQueryMetric.Lifecycle lifecycle = queryMetric.getLifecycle();
                Map<String,String> metricValues = queryMetricHandler.getEventFields(queryMetric);
                long createDate = queryMetric.getCreateDate().getTime();

                StringBuilder tagSb = new StringBuilder();
                Set<String> configuredMetricTags = queryMetricsWriterConfiguration.getTimelyMetricTags();
                for (String fieldName : configuredMetricTags) {
                    String fieldValue = metricValues.get(fieldName);
                    if (!StringUtils.isBlank(fieldValue)) {
                        // ensure that there are no spaces in tag values
                        fieldValue = fieldValue.replaceAll(" ", "_");
                        tagSb.append(fieldName).append("=").append(fieldValue).append(" ");
                    }
                }
                int tagSbLength = tagSb.length();
                if (tagSbLength > 0) {
                    if (tagSb.charAt(tagSbLength - 1) == ' ') {
                        tagSb.deleteCharAt(tagSbLength - 1);
                    }
                }
                tagSb.append("\n");

                timelyClient.open();

                if (lifecycle.equals(Lifecycle.RESULTS) || lifecycle.equals(Lifecycle.NEXTTIMEOUT) || lifecycle.equals(Lifecycle.MAXRESULTS)) {
                    List<PageMetric> pageTimes = queryMetric.getPageTimes();
                    // there should only be a maximum of one page metric as all but the last are removed by the QueryMetricsBean
                    for (PageMetric pm : pageTimes) {
                        Long lastPageSent = lastPageMetricMap.get(queryId);
                        // prevent duplicate reporting
                        if (lastPageSent == null || pm.getPageNumber() > lastPageSent) {
                            long requestTime = pm.getPageRequested();
                            long callTime = pm.getCallTime();
                            if (callTime == -1) {
                                callTime = pm.getReturnTime();
                            }
                            if (pm.getPagesize() > 0) {
                                timelyClient.write("put dw.query.metrics.PAGE_METRIC.calltime " + requestTime + " " + callTime + " " + tagSb);
                                String callTimePerRecord = df.format((double) callTime / pm.getPagesize());
                                timelyClient.write("put dw.query.metrics.PAGE_METRIC.calltimeperrecord " + requestTime + " " + callTimePerRecord + " " + tagSb);
                            }
                            lastPageMetricMap.put(queryId, pm.getPageNumber());

                        }
                    }
                }

                if (lifecycle.equals(Lifecycle.CLOSED) || lifecycle.equals(Lifecycle.CANCELLED)) {
                    // write ELAPSED_TIME
                    timelyClient.write("put dw.query.metrics.ELAPSED_TIME " + createDate + " " + queryMetric.getElapsedTime() + " " + tagSb);

                    // write NUM_RESULTS
                    timelyClient.write("put dw.query.metrics.NUM_RESULTS " + createDate + " " + queryMetric.getNumResults() + " " + tagSb);

                    // clean up last page map
                    lastPageMetricMap.remove(queryId);
                }

                if (lifecycle.equals(Lifecycle.INITIALIZED)) {
                    // write CREATE_TIME
                    long createTime = queryMetric.getCreateCallTime();
                    if (createTime == -1) {
                        createTime = queryMetric.getSetupTime();
                    }
                    timelyClient.write("put dw.query.metrics.CREATE_TIME " + createDate + " " + createTime + " " + tagSb);

                    // write a COUNT value of 1 so that we can count total queries
                    timelyClient.write("put dw.query.metrics.COUNT " + createDate + " 1 " + tagSb);
                }

            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }
}
