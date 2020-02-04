package datawave.webservice.query.metric;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.PostConstruct;
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
import javax.enterprise.event.Observes;
import javax.enterprise.inject.spi.BeanManager;
import javax.inject.Inject;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.Queue;

import datawave.configuration.DatawaveEmbeddedProjectStageHolder;
import datawave.configuration.RefreshEvent;
import datawave.configuration.spring.SpringBean;
import datawave.security.authorization.DatawavePrincipal;
import datawave.util.timely.UdpClient;
import datawave.webservice.common.connection.AccumuloConnectionFactory;
import datawave.webservice.query.metric.BaseQueryMetric.PageMetric;
import org.apache.commons.collections4.map.LRUMap;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.deltaspike.core.api.exclude.Exclude;
import org.apache.log4j.Logger;
import datawave.webservice.query.metric.BaseQueryMetric.Lifecycle;

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
    private AccumuloConnectionFactory connectionFactory;
    
    @Inject
    private QueryMetricHandler<? extends BaseQueryMetric> queryMetricHandler;
    
    @Inject
    @SpringBean(name = "QueryMetricsWriterConfiguration", refreshable = true)
    private QueryMetricsWriterConfiguration config;
    
    private UdpClient timelyClient = null;
    private Map<String,Long> lastPageMetricMap;
    
    // queryId to lastPage Map
    private Map<String,Long> lastPageMap;
    private List<QueryMetricHolder> metricQueue;
    private DecimalFormat df = new DecimalFormat("0.00");
    
    private static volatile AtomicBoolean receivingMetrics = new AtomicBoolean(false);
    
    private UdpClient createUdpClient() {
        if (config != null && StringUtils.isNotBlank(config.getTimelyHost())) {
            return new UdpClient(config.getTimelyHost(), config.getTimelyPort());
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
    
    @PostConstruct
    private void init() {
        // noinspection unchecked
        lastPageMap = new LRUMap(1000);
        lastPageMetricMap = new LRUMap(1000);
        metricQueue = new ArrayList<>();
        timelyClient = createUdpClient();
    }
    
    @Schedule(hour = "*", minute = "*", second = "*/10", persistent = false)
    public void receiveQueryMetrics() {
        
        if (receivingMetrics.compareAndSet(false, true)) {
            
            long start = System.currentTimeMillis();
            List<QueryMetricHolder> failedMetrics = new ArrayList<>();
            try {
                if (!metricQueue.isEmpty()) {
                    try {
                        // write previously failed metrics
                        failedMetrics = writeMetrics(queryMetricHandler, metricQueue);
                        int successful = metricQueue.size() - failedMetrics.size();
                        if (successful > 0) {
                            // logged at ERROR to record successful write of previously failed writes
                            log.error("Wrote " + successful + " previously failed query metric updates");
                        }
                        if (!failedMetrics.isEmpty()) {
                            throw new IllegalStateException(failedMetrics.size() + " metrics failed write");
                        }
                    } catch (Throwable t) {
                        log.error(failedMetrics.size() + " metric updates failed a second time, removing");
                        for (QueryMetricHolder h : failedMetrics) {
                            log.error("Failed write : " + h.getQueryMetric());
                        }
                    } finally {
                        metricQueue.clear();
                    }
                }
                
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
                                    }
                                    if (queryMetricHolder != null) {
                                        metricQueue.add(queryMetricHolder);
                                    }
                                }
                            } catch (Exception e) {
                                log.error(e.getMessage() + " messageID:" + message.getJMSMessageID());
                                continue;
                            }
                        }
                        // break out of loop every minute to ensure flush and acknowledge messages
                        if (metricQueue.size() >= 1000 || (System.currentTimeMillis() - start) > 60000) {
                            break;
                        }
                    } while (message != null);
                }
                
                failedMetrics = writeMetrics(queryMetricHandler, metricQueue);
                if (log.isTraceEnabled() && (metricQueue.size() - failedMetrics.size()) > 0) {
                    log.trace("Wrote " + (metricQueue.size() - failedMetrics.size()) + " query metric updates");
                }
                metricQueue.clear();
                if (!failedMetrics.isEmpty()) {
                    metricQueue.addAll(failedMetrics);
                    throw new IllegalStateException(metricQueue.size() + " metrics failed write");
                }
            } catch (Throwable t) {
                log.error(t.getMessage(), t);
                log.error("Error writing " + metricQueue.size() + " query metric updates.  Creating new queryMetricHandler.");
                // error during write or flush, create a new handler so that we can re-try next time
                queryMetricHandler.reload();
            } finally {
                try {
                    queryMetricHandler.flush();
                } catch (Throwable t) {
                    log.error(t.getMessage(), t);
                } finally {
                    receivingMetrics.set(false);
                }
            }
        }
    }
    
    private synchronized void sendMetricsToTimely(BaseQueryMetric queryMetric) {
        
        if (timelyClient != null && queryMetric.getQueryType().equalsIgnoreCase("RunningQuery")) {
            try {
                String queryId = queryMetric.getQueryId();
                BaseQueryMetric.Lifecycle lifecycle = queryMetric.getLifecycle();
                Map<String,String> metricValues = queryMetricHandler.getEventFields(queryMetric);
                long createDate = queryMetric.getCreateDate().getTime();
                
                StringBuilder tagSb = new StringBuilder();
                Set<String> configuredMetricTags = config.getTimelyMetricTags();
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
    
    private List<QueryMetricHolder> writeMetrics(QueryMetricHandler queryMetricHandler, List<QueryMetricHolder> metricQueue) throws Exception {
        
        List<QueryMetricHolder> failedMetrics = new ArrayList<>();
        
        if (!metricQueue.isEmpty()) {
            log.debug("writing " + metricQueue.size() + " query metric updates");
            for (QueryMetricHolder queryMetricHolder : metricQueue) {
                try {
                    BaseQueryMetric queryMetric = queryMetricHolder.getQueryMetric();
                    handleLegacyEvents(queryMetric);
                    DatawavePrincipal datawavePrincipal = queryMetricHolder.getPrincipal();
                    queryMetricHandler.updateMetric(queryMetric, datawavePrincipal);
                    sendMetricsToTimely(queryMetric);
                } catch (Throwable t) {
                    log.error("query metric updates failed: " + t.getMessage(), t);
                    failedMetrics.add(queryMetricHolder);
                }
            }
            try {
                queryMetricHandler.flush();
            } catch (Throwable t) {
                failedMetrics.addAll(metricQueue);
            }
            log.debug("wrote " + (metricQueue.size() - failedMetrics.size()) + " query metric updates");
        }
        return failedMetrics;
    }
    
    private void handleLegacyEvents(BaseQueryMetric queryMetric) {
        long lastUpdated;
        List<PageMetric> pages = queryMetric.getPageTimes();
        if (pages != null && !pages.isEmpty()) {
            // only old events should lack page numbers
            if (pages.get(0).getPageNumber() == -1) {
                
                Comparator<PageMetric> c = (m1, m2) -> {
                    CompareToBuilder builder = new CompareToBuilder();
                    builder.append(m1.getPageRequested(), m2.getPageRequested());
                    builder.append(m1.getPageReturned(), m2.getPageReturned());
                    return builder.toComparison();
                };
                
                // Sort by pageRequested and pageReturned
                Collections.sort(pages, c);
                
                // lastUpdated used to be set in the ShardTableQueryMetricHandler
                // Now it is done in the QueryMetricsBean.
                // If this message was stuck on the queue, then lastUpdated will not have been set yet.
                lastUpdated = pages.get(pages.size() - 1).getPageReturned();
                
                // if an older message is pulled off of the queue that does not have page numbers assigned,
                // then sort by pageRequested time and assign page numbers
                long x = 0;
                Long lastPage = lastPageMap.get(queryMetric.getQueryId());
                Iterator<PageMetric> itr = pages.iterator();
                while (itr.hasNext()) {
                    x++;
                    PageMetric p = itr.next();
                    // if we have a record of the last page recorded, then remove that page and all
                    // pages before it to limit the time necessary in updating
                    if (lastPage != null && x <= lastPage) {
                        itr.remove();
                    } else {
                        p.setPageNumber(x);
                    }
                }
                lastPageMap.put(queryMetric.getQueryId(), x);
                queryMetric.setLastUpdated(new Date(lastUpdated));
            }
        }
    }
}
