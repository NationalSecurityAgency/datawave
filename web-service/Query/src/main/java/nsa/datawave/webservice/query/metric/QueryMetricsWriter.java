package nsa.datawave.webservice.query.metric;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
import javax.inject.Inject;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.Queue;

import nsa.datawave.configuration.DatawaveEmbeddedProjectStageHolder;
import nsa.datawave.security.authorization.DatawavePrincipal;
import nsa.datawave.webservice.common.connection.AccumuloConnectionFactory;
import nsa.datawave.webservice.query.metric.BaseQueryMetric.PageMetric;
import org.apache.commons.collections.map.LRUMap;
import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.deltaspike.core.api.exclude.Exclude;
import org.apache.log4j.Logger;

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
    
    // queryId to lastPage Map
    private Map<String,Long> lastPageMap;
    private List<QueryMetricHolder> metricQueue;
    
    volatile static private AtomicBoolean receivingMetrics = new AtomicBoolean(false);
    
    @PostConstruct
    private void init() {
        // noinspection unchecked
        lastPageMap = new LRUMap(1000);
        metricQueue = new ArrayList<>();
    }
    
    @Schedule(hour = "*", minute = "*", second = "*/10", persistent = false)
    public void receiveQueryMetrics() {
        
        if (receivingMetrics.compareAndSet(false, true)) {
            
            long start = System.currentTimeMillis();
            List<QueryMetricHolder> failedMetrics = new ArrayList<>();
            try {
                if (metricQueue.size() > 0) {
                    try {
                        // write previously failed metrics
                        failedMetrics = writeMetrics(queryMetricHandler, metricQueue);
                        int successful = metricQueue.size() - failedMetrics.size();
                        if (successful > 0) {
                            // logged at ERROR to record successful write of previously failed writes
                            log.error("Wrote " + successful + " previously failed query metric updates");
                        }
                        if (failedMetrics.size() > 0) {
                            throw new IllegalStateException(failedMetrics.size() + " metrics failed write");
                        }
                    } catch (Throwable t) {
                        log.error(failedMetrics.size() + " metric updates failed a second time, removing");
                        for (QueryMetricHolder h : failedMetrics) {
                            log.error("Failed write : " + h.getQueryMetric().toString());
                        }
                    } finally {
                        metricQueue.clear();
                    }
                }
                
                JMSConsumer consumer = jmsContext.createConsumer(dest);
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
                
                failedMetrics = writeMetrics(queryMetricHandler, metricQueue);
                if (log.isTraceEnabled() && (metricQueue.size() - failedMetrics.size()) > 0) {
                    log.trace("Wrote " + (metricQueue.size() - failedMetrics.size()) + " query metric updates");
                }
                metricQueue.clear();
                if (failedMetrics.size() > 0) {
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
    
    private List<QueryMetricHolder> writeMetrics(QueryMetricHandler queryMetricHandler, List<QueryMetricHolder> metricQueue) throws Exception {
        
        List<QueryMetricHolder> failedMetrics = new ArrayList<>();
        
        if (metricQueue.size() > 0) {
            log.debug("writing " + metricQueue.size() + " query metric updates");
            for (QueryMetricHolder queryMetricHolder : metricQueue) {
                try {
                    BaseQueryMetric queryMetric = queryMetricHolder.getQueryMetric();
                    handleLegacyEvents(queryMetric);
                    DatawavePrincipal datawavePrincipal = queryMetricHolder.getPrincipal();
                    queryMetricHandler.updateMetric(queryMetric, datawavePrincipal);
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
                
                Comparator<PageMetric> c = new Comparator<PageMetric>() {
                    @Override
                    public int compare(PageMetric m1, PageMetric m2) {
                        CompareToBuilder builder = new CompareToBuilder();
                        builder.append(m1.getPageRequested(), m2.getPageRequested());
                        builder.append(m1.getPageReturned(), m2.getPageReturned());
                        return builder.toComparison();
                    }
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
