package datawave.webservice.query.metric;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.TreeMap;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlTransient;

import datawave.webservice.HtmlProvider;
import datawave.webservice.query.metric.BaseQueryMetric.PageMetric;
import datawave.webservice.result.BaseResponse;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;

public abstract class BaseQueryMetricListResponse<T extends BaseQueryMetric> extends BaseResponse implements HtmlProvider {
    
    private static final long serialVersionUID = 1L;
    private static final String TITLE = "Query Metrics";
    private static final String EMPTY = "";
    @XmlElementWrapper(name = "queryMetrics")
    @XmlElement(name = "queryMetric")
    protected List<T> result = null;
    @XmlElement
    protected int numResults = 0;
    @XmlTransient
    private boolean administratorMode = false;
    @XmlTransient
    private boolean isGeoQuery = false;
    
    private static String numToString(long number) {
        return (number == -1 || number == 0) ? "" : Long.toString(number);
    }
    
    public List<T> getResult() {
        return result;
    }
    
    public int getNumResults() {
        return numResults;
    }
    
    public void setResult(List<T> result) {
        this.result = result;
        this.numResults = this.result.size();
    }
    
    public void setNumResults(int numResults) {
        this.numResults = numResults;
    }
    
    public boolean isAdministratorMode() {
        return administratorMode;
    }
    
    public void setAdministratorMode(boolean administratorMode) {
        this.administratorMode = administratorMode;
    }
    
    public boolean isGeoQuery() {
        return isGeoQuery;
    }
    
    public void setGeoQuery(boolean geoQuery) {
        isGeoQuery = geoQuery;
    }
    
    @Override
    public String getTitle() {
        return TITLE;
    }
    
    @Override
    public String getPageHeader() {
        return getTitle();
    }
    
    @Override
    public String getHeadContent() {
        if (isGeoQuery) {
            // @formatter:off
            return "<script type='text/javascript' src='/jquery.min.js'></script>" +
                    "<script type='text/javascript'>" +
                    "$(document).ready(function() {" +
                    "   var queryHeader = document.getElementById(\"query-header\").innerHTML;" +
                    "   queryHeader = queryHeader + '<br>(<a href=\"' + window.location.href + '/map\">map</a>)';" +
                    "   document.getElementById(\"query-header\").innerHTML = queryHeader;" +
                    "});" +
                    "</script>";
            // @formatter: on
        } else {
            return EMPTY;
        }
    }
    
    @Override
    public String getMainContent() {
        StringBuilder builder = new StringBuilder();
        
        builder.append("<table>\n");
        builder.append("<tr>\n");
        builder.append("<th>Visibility</th><th>Query Date</th><th>User</th><th>UserDN</th><th>Proxy Server(s)</th><th>Query ID</th><th>Query Type</th>");
        builder.append("<th>Query Logic</th><th id=\"query-header\">Query</th><th>Begin Date</th><th>End Date</th><th>Query Auths</th><th>Server</th>");
        builder.append("<th>Query Setup Time (ms)</th><th>Query Setup Call Time (ms)</th><th>Number Pages</th><th>Number Results</th>");
        builder.append("<th>Total Page Time (ms)</th><th>Total Page Call Time (ms)</th><th>Total Page Serialization Time (ms)</th>");
        builder.append("<th>Total Page Bytes Sent (uncompressed)</th><th>Lifecycle</th><th>Elapsed Time</th><th>Error Code</th><th>Error Message</th>");
        builder.append("\n</tr>\n");
        
        TreeMap<Date,T> metricMap = new TreeMap<Date,T>(Collections.reverseOrder());
        
        for (T metric : this.getResult()) {
            metricMap.put(metric.getCreateDate(), metric);
        }
        
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HHmmss");
        
        int x = 0;
        for (T metric : metricMap.values()) {
            // highlight alternating rows
            if (x % 2 == 0) {
                builder.append("<tr class=\"highlight\" style=\"vertical-align:top;\">\n");
            } else {
                builder.append("<tr style=\"vertical-align:top;\">\n");
            }
            x++;
            
            builder.append("<td>").append(metric.getColumnVisibility()).append("</td>");
            builder.append("<td style=\"min-width:125px !important;\">").append(sdf.format(metric.getCreateDate())).append("</td>");
            builder.append("<td>").append(metric.getUser()).append("</td>");
            String userDN = metric.getUserDN();
            builder.append("<td style=\"min-width:500px !important;\">").append(userDN == null ? "" : userDN).append("</td>");
            String proxyServers = metric.getProxyServers() == null ? "" : StringUtils.join(metric.getProxyServers(), "<BR/>");
            builder.append("<td>").append(proxyServers).append("</td>");
            if (this.isAdministratorMode()) {
                builder.append("<td><a href=\"/DataWave/Query/Metrics/user/").append(metric.getUser()).append("/").append(metric.getQueryId()).append("/")
                                .append("\">").append(metric.getQueryId()).append("</a></td>");
            } else {
                builder.append("<td><a href=\"/DataWave/Query/Metrics/id/").append(metric.getQueryId()).append("/").append("\">").append(metric.getQueryId())
                                .append("</a></td>");
            }
            builder.append("<td>").append(metric.getQueryType()).append("</td>");
            builder.append("<td>").append(metric.getQueryLogic()).append("</td>");
            builder.append("<td style=\"word-wrap: break-word;\">").append(StringEscapeUtils.escapeHtml(metric.getQuery())).append("</td>");
            
            String beginDate = metric.getBeginDate() == null ? "" : sdf.format(metric.getBeginDate());
            builder.append("<td style=\"min-width:125px !important;\">").append(beginDate).append("</td>");
            String endDate = metric.getEndDate() == null ? "" : sdf.format(metric.getEndDate());
            builder.append("<td style=\"min-width:125px !important;\">").append(endDate).append("</td>");
            String queryAuths = metric.getQueryAuthorizations() == null ? "" : metric.getQueryAuthorizations().replaceAll(",", " ");
            builder.append("<td style=\"word-wrap: break-word; min-width:300px !important;\">").append(queryAuths).append("</td>");
            
            builder.append("<td>").append(metric.getHost()).append("</td>");
            builder.append("<td>").append(metric.getSetupTime()).append("</td>");
            builder.append("<td>").append(numToString(metric.getCreateCallTime())).append("</td>\n");
            builder.append("<td>").append(metric.getNumPages()).append("</td>");
            builder.append("<td>").append(metric.getNumResults()).append("</td>");
            long count = 0l;
            long callTime = 0l;
            long serializationTime = 0l;
            long bytesSent = 0l;
            for (PageMetric p : metric.getPageTimes()) {
                count += p.getReturnTime();
                callTime += (p.getCallTime()) == -1 ? 0 : p.getCallTime();
                serializationTime += (p.getSerializationTime()) == -1 ? 0 : p.getSerializationTime();
                bytesSent += (p.getBytesWritten()) == -1 ? 0 : p.getBytesWritten();
            }
            builder.append("<td>").append(count).append("</td>\n");
            builder.append("<td>").append(numToString(callTime)).append("</td>\n");
            builder.append("<td>").append(numToString(serializationTime)).append("</td>\n");
            builder.append("<td>").append(numToString(bytesSent)).append("</td>\n");
            builder.append("<td>").append(metric.getLifecycle()).append("</td>");
            builder.append("<td>").append(metric.getElapsedTime()).append("</td>");
            String errorCode = metric.getErrorCode();
            builder.append("<td style=\"word-wrap: break-word;\">").append((errorCode == null) ? "" : StringEscapeUtils.escapeHtml(errorCode)).append("</td>");
            String errorMessage = metric.getErrorMessage();
            builder.append("<td style=\"word-wrap: break-word;\">").append((errorMessage == null) ? "" : StringEscapeUtils.escapeHtml(errorMessage))
                            .append("</td>");
            
            builder.append("\n</tr>\n");
        }
        
        builder.append("</table>\n");
        
        return builder.toString();
    }
    
}
