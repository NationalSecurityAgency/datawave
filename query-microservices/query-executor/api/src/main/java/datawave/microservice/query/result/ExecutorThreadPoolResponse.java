package datawave.microservice.query.result;

import datawave.services.common.result.ConnectionPool;
import datawave.webservice.HtmlProvider;
import datawave.webservice.result.BaseResponse;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import java.text.NumberFormat;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

@XmlRootElement(name = "ConnectionFactoryResponse")
@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class ExecutorThreadPoolResponse extends BaseResponse implements HtmlProvider {
    
    private static final long serialVersionUID = 1L;
    private static final String TITLE = "Executor Thread Pool Metrics", EMPTY = "";
    
    @XmlElement(name = "pool")
    private String pool;
    
    @XmlElementWrapper(name = "ConnectionPools")
    @XmlElement(name = "ConnectionPool")
    private List<ConnectionPool> connectionPools = new LinkedList<>();
    
    @XmlElement(name = "threadPoolStatus")
    private Map<String,String> threadPoolStatus = new HashMap<>();
    
    @XmlElement(name = "queryToTask")
    private Map<String,Collection<QueryTaskDescription>> queryToTask = new HashMap<>();
    
    @Override
    public String getTitle() {
        return TITLE + " for " + pool;
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
        
        builder.append("</style>\n");
        
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
        
        builder.append("<br/>");
        
        builder.append("<h2>").append("Connection Pools").append("</h2>");
        builder.append("<br/>");
        builder.append("<table class=\"connectionPools\">");
        builder.append("<tr><th>Priority</th><th>Num Active</th><th>Max Active</th><th>Num Idle</th><th>Max Idle</th><th>Num Waiting</th></tr>");
        
        for (ConnectionPool f : connectionPools) {
            if (f.getPoolName().equals(pool)) {
                builder.append("<tr>");
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
        
        builder.append("<h2>").append("Query Tasks").append("</h2>");
        builder.append("<table class=\"queryTasks\">");
        builder.append("<tr><th>Query</th><th>Task</th><th>Method</th><th>State</th></tr>");
        
        for (Map.Entry<String,Collection<QueryTaskDescription>> entry : queryToTask.entrySet()) {
            builder.append("<tr>");
            builder.append("<td class=\"allBorders\"><a href=\"/DataWave/Query/Metrics/id/").append(entry.getKey()).append("/").append("\">")
                            .append(entry.getKey()).append("</a></td>");
            builder.append("<td>").append(entry.getValue().size()).append("</td>");
            builder.append("</tr>");
            
            for (QueryTaskDescription task : entry.getValue()) {
                builder.append("<tr>");
                builder.append("<td></td>");
                builder.append("<td>").append(task.getTaskId()).append("</td>");
                builder.append("<td>").append(task.getMethod()).append("</td>");
                builder.append("<td>").append(task.getState()).append("</td>");
                builder.append("</tr>");
            }
        }
        builder.append("</table>");
        
        return builder.toString();
    }
    
    public List<ConnectionPool> getConnectionPools() {
        return connectionPools;
    }
    
    public void setConnectionPools(List<ConnectionPool> connectionPool) {
        this.connectionPools.clear();
        this.connectionPools.addAll(connectionPool);
    }
    
    public Map<String,String> getThreadPoolStatus() {
        return threadPoolStatus;
    }
    
    public void setThreadPoolStatus(Map<String,String> threadPoolStatus) {
        this.threadPoolStatus.clear();
        this.threadPoolStatus.putAll(threadPoolStatus);
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
        this.queryToTask.clear();
        this.queryToTask.putAll(queryToTask);
    }
}
