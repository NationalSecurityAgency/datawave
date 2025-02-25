package datawave.microservice.query.result;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.commons.lang.StringEscapeUtils;

import datawave.core.common.result.Connection;
import datawave.core.common.result.ConnectionPool;
import datawave.webservice.HtmlProvider;
import datawave.webservice.result.BaseResponse;

@XmlRootElement(name = "ConnectionFactoryResponse")
@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class ExecutorMetricsResponse extends BaseResponse implements HtmlProvider {
    
    private static final long serialVersionUID = 1L;
    private static final String EMPTY = "";
    
    @XmlElement
    private String title;
    
    @XmlElement
    private String queryMetricsUrlPrefix;
    
    @XmlElement(name = "pool")
    private String pool;
    
    @XmlElementWrapper(name = "ConnectionPools")
    @XmlElement(name = "ConnectionPool")
    private List<ConnectionPool> connectionPools;
    
    @XmlElement(name = "threadPoolStatus")
    private Map<String,String> threadPoolStatus;
    
    @XmlElement(name = "queryToTask")
    private Map<String,Collection<QueryTaskDescription>> queryToTask;
    
    @Override
    public String getTitle() {
        return title;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see datawave.webservice.HtmlProvider#getPageHeader()
     */
    @Override
    public String getPageHeader() {
        return getTitle();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see datawave.webservice.HtmlProvider#getHeadContent()
     */
    @Override
    public String getHeadContent() {
        return EMPTY;
    }
    
    @Override
    public String getMainContent() {
        
        NumberFormat formatter = NumberFormat.getInstance();
        formatter.setGroupingUsed(true);
        formatter.setMaximumFractionDigits(2);
        formatter.setParseIntegerOnly(false);
        
        StringBuilder builder = new StringBuilder();
        
        builder.append("<style>\n");
        
        builder.append("table.threadPool td, table.threadPool th {");
        builder.append("border-colapse: collapse;\n");
        builder.append("border: 1px black solid;\n");
        builder.append("empty-cells: hide;\n");
        builder.append("}\n");
        
        builder.append("\n");
        
        builder.append("table.queryTasks td, table.queryTasks th {");
        builder.append("border-colapse: collapse;\n");
        builder.append("border: 1px black solid;\n");
        builder.append("word-wrap: break-word;\n");
        builder.append("empty-cells: hide;\n");
        builder.append("}\n");
        
        builder.append("\n");
        
        builder.append("table.connectionPools td, table.connectionPools th {");
        builder.append("border-colapse: collapse;\n");
        builder.append("border: 1px black solid;\n");
        builder.append("empty-cells: hide;\n");
        builder.append("}\n");
        
        builder.append("\n");
        
        builder.append("table.connectionRequests td, table.connectionRequests th {");
        builder.append("border-colapse: collapse;\n");
        builder.append("border: 1px black solid;\n");
        builder.append("word-wrap: break-word;\n");
        builder.append("empty-cells: hide;\n");
        builder.append("}\n");
        
        builder.append("</style>\n");
        
        boolean hasSection = false;
        
        if (threadPoolStatus != null) {
            if (hasSection) {
                builder.append("<br/>");
            }
            hasSection = true;
            builder.append("<h2>").append("Thread Pool").append("</h2>");
            builder.append("<br/>");
            builder.append("<table class=\"threadPool\">");
            builder.append("<tr><th>Property</th><th>Value</th></tr>");
            
            for (Map.Entry<String,String> entry : threadPoolStatus.entrySet()) {
                builder.append("<tr>");
                builder.append("<td>").append(entry.getKey()).append("</td>");
                builder.append("<td>").append(entry.getValue()).append("</td>");
                builder.append("</tr>");
            }
            
            builder.append("</table>");
        }
        
        if (queryToTask != null) {
            if (hasSection) {
                builder.append("<br/>");
            }
            hasSection = true;
            builder.append("<h2>").append("Query Tasks").append("</h2>");
            builder.append("<table class=\"queryTasks\">");
            builder.append("<tr><th>Query</th><th># Tasks<th>Task Id</th><th>Method</th><th>State</th></tr>");
            
            for (Map.Entry<String,Collection<QueryTaskDescription>> entry : queryToTask.entrySet()) {
                builder.append("<tr>");
                if (queryMetricsUrlPrefix != null && !queryMetricsUrlPrefix.isEmpty()) {
                    builder.append("<td class=\"allBorders\"><a href=\"").append(queryMetricsUrlPrefix).append(entry.getKey()).append("/").append("\">")
                                    .append(entry.getKey()).append("</a></td>");
                } else {
                    builder.append("<td>").append(entry.getKey()).append("</td>");
                }
                builder.append("<td>").append(entry.getValue().size()).append("</td>");
                boolean newLine = false;
                
                for (QueryTaskDescription task : entry.getValue()) {
                    if (newLine) {
                        builder.append("<tr>");
                        builder.append("<td></td>");
                        builder.append("<td></td>");
                    }
                    newLine = true;
                    builder.append("<td>").append(task.getTaskId()).append("</td>");
                    builder.append("<td>").append(task.getMethod()).append("</td>");
                    builder.append("<td>").append(task.getState()).append("</td>");
                    builder.append("</tr>");
                }
            }
            builder.append("</table>");
        }
        
        if (connectionPools != null) {
            if (hasSection) {
                builder.append("<br/>");
            }
            hasSection = true;
            
            builder.append("<h2>").append("Connection Pools").append("</h2>");
            builder.append("<br/>");
            builder.append("<table class=\"connectionPools\">");
            if (pool == null) {
                builder.append("<tr><th>Pool Name</th><th>Priority</th><th>Num Active</th><th>Max Active</th><th>Num Idle</th><th>Max Idle</th><th>Num Waiting</th></tr>");
            } else {
                builder.append("<tr><th>Priority</th><th>Num Active</th><th>Max Active</th><th>Num Idle</th><th>Max Idle</th><th>Num Waiting</th></tr>");
            }
            
            Set<ConnectionPool> poolSet = new TreeSet<>();
            poolSet.addAll(connectionPools);
            
            for (ConnectionPool f : poolSet) {
                if (pool == null || f.getPoolName().equals(pool)) {
                    builder.append("<tr>");
                    if (pool == null) {
                        builder.append("<td>").append(f.getPoolName()).append("</td>");
                    }
                    builder.append("<td>").append(f.getPriority()).append("</td>");
                    builder.append("<td>").append(f.getNumActive()).append("</td>");
                    builder.append("<td>").append(f.getMaxActive()).append("</td>");
                    builder.append("<td>").append(f.getNumIdle()).append("</td>");
                    builder.append("<td>").append(f.getMaxIdle()).append("</td>");
                    builder.append("<td>").append(f.getNumWaiting()).append("</td>");
                    builder.append("</tr>");
                }
            }
            builder.append("</table>");
            
            builder.append("<br/>");
            
            builder.append("<h2>").append("ConnectionRequests").append("</h2>");
            builder.append("<table class=\"connectionRequests\">");
            if (pool == null) {
                builder.append("<tr><th>Pool Name</th><th>Priority</th><th>State</th><th>Time In State (ms)</th><th>Key</th><th>Value</th></tr>");
            } else {
                builder.append("<tr><th>Priority</th><th>State</th><th>Time In State (ms)</th><th>Key</th><th>Value</th></tr>");
            }
            
            for (ConnectionPool f : connectionPools) {
                if (pool == null || f.getPoolName().equals(pool)) {
                    
                    List<Connection> connectionRequests = f.getConnectionRequests();
                    
                    if (connectionRequests != null) {
                        for (Connection h : connectionRequests) {
                            
                            builder.append("<tr>");
                            if (pool == null) {
                                builder.append("<td>").append(f.getPoolName()).append("</td>");
                            }
                            builder.append("<td>").append(f.getPriority()).append("</td>");
                            builder.append("<td>").append(h.getState()).append("</td>");
                            builder.append("<td>").append(h.getTimeInState()).append("</td>");
                            String requestLocation = h.getRequestLocation();
                            if (requestLocation == null) {
                                builder.append("<td></td>");
                                builder.append("<td></td>");
                            } else {
                                builder.append("<td>request.location</td>");
                                builder.append("<td>").append(h.getRequestLocation()).append("</td>");
                            }
                            builder.append("</tr>");
                            
                            Map<String,String> properties = h.getConnectionPropertiesAsMap();
                            String queryUser = properties.get("query.user");
                            
                            for (Map.Entry<String,String> e : properties.entrySet()) {
                                
                                String name = e.getKey();
                                String value = e.getValue();
                                
                                builder.append("<tr>");
                                builder.append("<td></td>");
                                builder.append("<td></td>");
                                builder.append("<td></td>");
                                builder.append("<td></td>");
                                builder.append("<td class=\"allBorders\">").append(StringEscapeUtils.escapeHtml(name)).append("</td>");
                                if (name.equals("query.id") && queryUser != null) {
                                    if (queryMetricsUrlPrefix != null && !queryMetricsUrlPrefix.isEmpty()) {
                                        builder.append("<td class=\"allBorders\"><a href=\"").append(queryMetricsUrlPrefix).append(value).append("/")
                                                        .append("\">").append(value).append("</a></td>");
                                    } else {
                                        builder.append("<td>").append(value).append("</td>");
                                    }
                                } else {
                                    builder.append("<td class=\"allBorders\">").append(StringEscapeUtils.escapeHtml(e.getValue().toString())).append("</td>");
                                }
                                builder.append("</tr>");
                            }
                        }
                    }
                }
            }
            builder.append("</table>");
        }
        
        return builder.toString();
    }
    
    public List<ConnectionPool> getConnectionPools() {
        return connectionPools;
    }
    
    public void setConnectionPools(List<ConnectionPool> connectionPools) {
        if (connectionPools == null) {
            this.connectionPools = null;
        } else {
            this.connectionPools = new ArrayList<>(connectionPools);
        }
    }
    
    public Map<String,String> getThreadPoolStatus() {
        return threadPoolStatus;
    }
    
    public void setThreadPoolStatus(Map<String,String> threadPoolStatus) {
        if (threadPoolStatus == null) {
            this.threadPoolStatus = null;
        } else {
            this.threadPoolStatus = new HashMap<>(threadPoolStatus);
        }
    }
    
    public String getQueryMetricsUrlPrefix() {
        return queryMetricsUrlPrefix;
    }
    
    public void setQueryMetricsUrlPrefix(String queryMetricsUrlPrefix) {
        this.queryMetricsUrlPrefix = queryMetricsUrlPrefix;
    }
    
    public String getPool() {
        return pool;
    }
    
    public void setPool(String pool) {
        this.pool = pool;
    }
    
    public Map<String,Collection<QueryTaskDescription>> getQueryToTask() {
        return queryToTask;
    }
    
    public void setQueryToTask(Map<String,Collection<QueryTaskDescription>> queryToTask) {
        if (queryToTask == null) {
            this.queryToTask = null;
        } else {
            this.queryToTask = new HashMap<>(queryToTask);
        }
    }
    
    public void setTitle(String title) {
        this.title = title;
    }
}
