package datawave.webservice.query.metric;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import datawave.webservice.HtmlProvider;
import datawave.webservice.query.exception.QueryExceptionType;
import datawave.webservice.result.BaseResponse;

import java.text.NumberFormat;
import java.util.List;

@XmlRootElement(name = "QueryMetricsSummaryResponse")
@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class QueryMetricsSummaryResponse extends BaseResponse implements HtmlProvider {
    
    private static final long serialVersionUID = 1L;
    private static final String TITLE = "Query Metrics Summary", EMPTY = "";
    private static final String START_CELL = "<td>", END_CELL = "</td>";
    
    @XmlElement(name = "OneHour")
    protected QueryMetricSummary hour1 = new QueryMetricSummary();
    @XmlElement(name = "SixHours")
    protected QueryMetricSummary hour6 = new QueryMetricSummary();
    @XmlElement(name = "TwelveHours")
    protected QueryMetricSummary hour12 = new QueryMetricSummary();
    @XmlElement(name = "OneDay")
    protected QueryMetricSummary day1 = new QueryMetricSummary();
    @XmlElement(name = "SevenDays")
    protected QueryMetricSummary day7 = new QueryMetricSummary();
    @XmlElement(name = "ThirtyDays")
    protected QueryMetricSummary day30 = new QueryMetricSummary();
    @XmlElement(name = "SixtyDays")
    protected QueryMetricSummary day60 = new QueryMetricSummary();
    @XmlElement(name = "NinetyDays")
    protected QueryMetricSummary day90 = new QueryMetricSummary();
    @XmlElement(name = "All")
    protected QueryMetricSummary all = new QueryMetricSummary();
    
    public QueryMetricSummary getHour1() {
        return hour1;
    }
    
    public void setHour1(QueryMetricSummary hour1) {
        this.hour1 = hour1;
    }
    
    public QueryMetricSummary getHour6() {
        return hour6;
    }
    
    public void setHour6(QueryMetricSummary hour6) {
        this.hour6 = hour6;
    }
    
    public QueryMetricSummary getHour12() {
        return hour12;
    }
    
    public void setHour12(QueryMetricSummary hour12) {
        this.hour12 = hour12;
    }
    
    public QueryMetricSummary getDay1() {
        return day1;
    }
    
    public void setDay1(QueryMetricSummary day1) {
        this.day1 = day1;
    }
    
    public QueryMetricSummary getDay7() {
        return day7;
    }
    
    public void setDay7(QueryMetricSummary day7) {
        this.day7 = day7;
    }
    
    public QueryMetricSummary getDay30() {
        return day30;
    }
    
    public void setDay30(QueryMetricSummary day30) {
        this.day30 = day30;
    }
    
    public QueryMetricSummary getDay60() {
        return day60;
    }
    
    public void setDay60(QueryMetricSummary day60) {
        this.day60 = day60;
    }
    
    public QueryMetricSummary getDay90() {
        return day90;
    }
    
    public void setDay90(QueryMetricSummary day90) {
        this.day90 = day90;
    }
    
    public QueryMetricSummary getAll() {
        return all;
    }
    
    public void setAll(QueryMetricSummary all) {
        this.all = all;
    }
    
    @Override
    public String getTitle() {
        return TITLE;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see datawave.webservice.HtmlProvider#getPageHeader()
     */
    @Override
    public String getPageHeader() {
        return (getExceptions() == null || getExceptions().isEmpty()) ? getTitle() : EMPTY;
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
    
    protected StringBuilder addSummary(StringBuilder builder, QueryMetricSummary summary, String name, boolean highlight) {
        NumberFormat formatter = NumberFormat.getInstance();
        if (highlight) {
            builder.append("<tr class=\"highlight\">");
        } else {
            builder.append("<tr>");
        }
        builder.append(START_CELL).append(name).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(summary.getQueryCount())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(summary.getTotalPages())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(summary.getTotalPageResultSize())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(summary.getMinPageResultSize())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(summary.getMaxPageResultSize())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(summary.getAvgPageResultSize())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(summary.getTotalPageResponseTime())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(summary.getMinPageResponseTime())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(summary.getMaxPageResponseTime())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(summary.getAvgPageResponseTime())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(summary.getAverageResultsPerSecond())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(summary.getAveragePagesPerSecond())).append(END_CELL);
        builder.append("</tr>\n");
        return builder;
    }
    
    @Override
    public String getMainContent() {
        
        StringBuilder builder = new StringBuilder();
        if (getExceptions() == null || getExceptions().isEmpty()) {
            NumberFormat formatter = NumberFormat.getInstance();
            formatter.setGroupingUsed(true);
            formatter.setMaximumFractionDigits(2);
            formatter.setParseIntegerOnly(false);
            
            builder.append("<table>");
            builder.append("<tr><th>Interval</th>");
            builder.append("<th>Queries Submitted</th>");
            builder.append("<th>Pages Returned</th>");
            builder.append("<th>Total Results</th>");
            builder.append("<th>Min Page Size</th>");
            builder.append("<th>Max Page Size</th>");
            builder.append("<th>Avg Page Size</th>");
            builder.append("<th>Total Page Response Time (ms)</th>");
            builder.append("<th>Min Page Response Time (ms)</th>");
            builder.append("<th>Max Page Response Time (ms)</th>");
            builder.append("<th>Avg Page Response Time (ms)</th>");
            builder.append("<th>Avg Results Per Second</th>");
            builder.append("<th>Avg Pages Per Second</th></tr>");
            addSummary(builder, hour1, "1 hour", false);
            addSummary(builder, hour6, "6 hours", true);
            addSummary(builder, hour12, "12 hours", false);
            addSummary(builder, day1, "1 day", true);
            addSummary(builder, day7, "7 day", false);
            addSummary(builder, day30, "30 days", true);
            addSummary(builder, day60, "60 days", false);
            addSummary(builder, day90, "90 days", true);
            addSummary(builder, all, "all", false);
            builder.append("</table>");
        } else {
            builder.append("<b>EXCEPTIONS:</b>").append("<br/>");
            List<QueryExceptionType> exceptions = getExceptions();
            if (exceptions != null) {
                for (QueryExceptionType exception : exceptions) {
                    if (exception != null)
                        builder.append(exception).append(", ").append(QueryExceptionType.getSchema()).append("<br/>");
                }
            }
        }
        return builder.toString();
    }
}
