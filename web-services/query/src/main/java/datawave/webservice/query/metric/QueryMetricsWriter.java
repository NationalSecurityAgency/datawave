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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import javax.annotation.security.PermitAll;
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

import org.apache.commons.collections4.map.LRUMap;
import org.apache.commons.lang.StringUtils;
import org.apache.deltaspike.core.api.exclude.Exclude;
import org.apache.log4j.Logger;

import datawave.configuration.DatawaveEmbeddedProjectStageHolder;
import datawave.configuration.RefreshEvent;
import datawave.configuration.spring.SpringBean;
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
@PermitAll
@LocalBean
@Lock(LockType.READ)
@Exclude(ifProjectStage = DatawaveEmbeddedProjectStageHolder.DatawaveEmbedded.class)
public class QueryMetricsWriter {

    private Logger log = Logger.getLogger(QueryMetricsWriter.class);

    @Resource(mappedName = "java:jboss/ee/concurrency/factory/default")
    private ManagedThreadFactory managedThreadFactory;

    @Inject
    private QueryMetricHandler<? extends BaseQueryMetric> queryMetricHandler;

    @Inject
    private RemoteQueryMetricService remoteQueryMetricService;

    @Inject
    @SpringBean(name = "QueryMetricsWriterConfiguration", refreshable = true)
    private QueryMetricsWriterConfiguration writerConfig;

    private UdpClient timelyClient = null;
    private Map<String,Long> lastPageMetricMap;

    private DecimalFormat df = new DecimalFormat("0.00");
    private Future future = null;
    private volatile boolean shuttingDown = false;
    private List<FailureRecord> failedMetrics = new ArrayList<>();
    private LinkedBlockingQueue<QueryMetricHolder> blockingQueue;

    @PostConstruct
    private void init() {
        // noinspection unchecked
        lastPageMetricMap = new LRUMap(1000);
        timelyClient = createUdpClient();
        blockingQueue = new LinkedBlockingQueue(writerConfig.getMaxQueueSize());
        ExecutorService executorService = Executors.newSingleThreadExecutor(managedThreadFactory);
        future = executorService.submit(new MetricProcessor());
    }

    @PreDestroy
    public void shutdown() {
        // try to ensure that the task running on the managed thread exits before shutdown
        long start = System.currentTimeMillis();
        this.shuttingDown = true;
        try {
            this.future.get(writerConfig.getMaxShutdownMs(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {

        }

        long remainingMs = writerConfig.getMaxShutdownMs() - (System.currentTimeMillis() - start);
        while (!blockingQueue.isEmpty() && failedMetrics.isEmpty() && remainingMs > 0) {
            List<QueryMetricHolder> metricHolderList = getMetricsFromQueue(100, Math.min(5000, remainingMs));
            log.info(String.format("writing %d metric updates while shutting down", metricHolderList.size()));
            processQueryMetrics(metricHolderList);
            remainingMs = writerConfig.getMaxShutdownMs() - (System.currentTimeMillis() - start);
        }
        log.info(String.format("shut down with %d metric updates in queue and %d failed metric updates", blockingQueue.size(), failedMetrics.size()));
    }

    private UdpClient createUdpClient() {
        if (writerConfig != null && StringUtils.isNotBlank(writerConfig.getTimelyHost())) {
            return new UdpClient(writerConfig.getTimelyHost(), writerConfig.getTimelyPort());
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

    public void addMetricToQueue(QueryMetricHolder queryMetricHolder) {
        try {
            if (shuttingDown) {
                log.error(String.format("shutting down - dropping metric for queryId %s", queryMetricHolder.getQueryMetric().getQueryId()));
            } else if (!blockingQueue.offer(queryMetricHolder)) {
                if (queryMetricHolder.getQueryMetric().getLifecycle().compareTo(Lifecycle.INITIALIZED) <= 0) {
                    String queryId = queryMetricHolder.getQueryMetric().getQueryId();
                    log.error(String.format("metric queue limit reached (%d/%d), dropping metric for queryId %s", blockingQueue.size(),
                                    writerConfig.getMaxQueueSize(), queryId));
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private List<QueryMetricHolder> getMetricsFromQueue(int batchSize, long maxLatency) {
        List metricHolderList = new ArrayList<>();
        long start = System.currentTimeMillis();
        while (metricHolderList.size() < batchSize && (System.currentTimeMillis() - start) < maxLatency) {
            try {
                QueryMetricHolder holder = blockingQueue.poll(100, TimeUnit.MILLISECONDS);
                if (holder != null) {
                    metricHolderList.add(holder);
                }
            } catch (InterruptedException e) {

            }
        }
        if (metricHolderList.size() > 0 || blockingQueue.size() > 0) {
            log.debug(String.format("retrieved %d metric updates from queue, %d remaining", metricHolderList.size(), blockingQueue.size()));
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
            while (!shuttingDown) {
                try {
                    // only process metrics if they are being successfully written
                    if (failedMetrics.isEmpty()) {
                        processQueryMetrics(getMetricsFromQueue(writerConfig.getBatchSize(), writerConfig.getMaxLatencyMs()));
                    } else {
                        if (!writeFailedMetrics()) {
                            log.error(String.format("Unable to write %d previously failed metrics, not reading from blockingQueue size %d",
                                            failedMetrics.size(), blockingQueue.size()));
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

    private void processQueryMetrics(List<QueryMetricHolder> metricHolderList) {
        if (!metricHolderList.isEmpty()) {
            if (writerConfig.getUseRemoteService()) {
                processQueryMetricsWithRemoteService(metricHolderList);
            } else {
                processQueryMetricsWithHandler(metricHolderList);
            }
        }
    }

    private void processQueryMetricsWithRemoteService(List<QueryMetricHolder> metricHolderList) {
        List<BaseQueryMetric> metricList = metricHolderList.stream().map(QueryMetricHolder::getQueryMetric).collect(Collectors.toList());
        if (!metricList.isEmpty()) {
            try {
                writeMetricsToRemoteService(metricList);
                log.debug(String.format("wrote %d metric updates to RemoteQueryMetricService", metricList.size()));
            } catch (Exception e) {
                log.error(String.format("%d metric updates failed write to RemoteQueryMetricService as a batch, will retry individually - %s",
                                metricList.size(), e.getMessage()), e);
                metricList.forEach(m -> {
                    failedMetrics.add(new FailureRecord(m, false));
                });
            }
        }
    }

    private void processQueryMetricsWithHandler(List<QueryMetricHolder> metricHolderList) {
        List<QueryMetricHolder> currentFailures = new ArrayList<>();
        AtomicBoolean anySuccess = new AtomicBoolean(false);
        try {
            if (!metricHolderList.isEmpty()) {
                currentFailures.addAll(writeMetricsToHandler(queryMetricHandler, metricHolderList));
                log.debug(String.format("wrote %d metric updates to QueryMetricHandler", (metricHolderList.size() - currentFailures.size())));
                anySuccess.set(currentFailures.size() < metricHolderList.size());
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            currentFailures.addAll(metricHolderList);
        } finally {
            if (!metricHolderList.isEmpty()) {
                if (!currentFailures.isEmpty()) {
                    log.error(String.format("%d metric updates failed write to QueryMetricHandler, will retry individually", currentFailures.size()));
                    currentFailures.stream().forEach(queryMetricHolder -> {
                        this.failedMetrics.add(new FailureRecord(queryMetricHolder, anySuccess.get()));
                    });
                }
                if (!anySuccess.get()) {
                    // error during write or flush, create a new handler so that we can re-try next time
                    log.error(String.format("error writing %d metric updates - creating new QueryMetricHandler", metricHolderList.size()));
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
            if (writerConfig.getUseRemoteService()) {
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
        String destination = writerConfig.getUseRemoteService() ? "RemoteQueryMetricService" : "QueryMetricHandler";
        log.debug(String.format("wrote %d previously failed metric updates to %s with %d failures", successful, destination, failedMetrics.size()));
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
                log.trace(String.format("failures individual/total: %d/%d for metric update %s", iFailures, tFailures, queryId));
            }
        }
        if (discardForFailureCount > 0) {
            log.error(String.format("discarding %d metric updates from %s for repeated failures", discardForFailureCount, discardForFailureMetrics));
        }
        if (discardForTimeCount > 0) {
            log.error(String.format("discarding %d metric updates from %s for exceeding max time in failure queue", discardForTimeCount,
                            discardForTimeMetrics));
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
                    log.error(String.format("metric update write to QueryMetricHandler failed: %s", e.getMessage()));
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
                Set<String> configuredMetricTags = writerConfig.getTimelyMetricTags();
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
                                timelyClient.write(String.format("put dw.query.metrics.PAGE_METRIC.calltime %d %d %s", requestTime, callTime, tagSb));
                                String callTimePerRecord = df.format((double) callTime / pm.getPagesize());
                                timelyClient.write(String.format("put dw.query.metrics.PAGE_METRIC.calltimeperrecord %d %s %s", requestTime, callTimePerRecord,
                                                tagSb));
                            }
                            lastPageMetricMap.put(queryId, pm.getPageNumber());

                        }
                    }
                }

                if (lifecycle.equals(Lifecycle.CLOSED) || lifecycle.equals(Lifecycle.CANCELLED)) {
                    // write ELAPSED_TIME
                    timelyClient.write(String.format("put dw.query.metrics.ELAPSED_TIME %d %d %s", createDate, queryMetric.getElapsedTime(), tagSb));

                    // write NUM_RESULTS
                    timelyClient.write(String.format("put dw.query.metrics.NUM_RESULTS %d %d %s", createDate, queryMetric.getNumResults(), tagSb));

                    // clean up last page map
                    lastPageMetricMap.remove(queryId);
                }

                if (lifecycle.equals(Lifecycle.INITIALIZED)) {
                    // write CREATE_TIME
                    long createTime = queryMetric.getCreateCallTime();
                    if (createTime == -1) {
                        createTime = queryMetric.getSetupTime();
                    }
                    timelyClient.write(String.format("put dw.query.metrics.CREATE_TIME %d %d %s", createDate, createTime, tagSb));

                    // write a COUNT value of 1 so that we can count total queries
                    timelyClient.write(String.format("put dw.query.metrics.COUNT %d 1 %s", createDate, tagSb));
                }

            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }
}
