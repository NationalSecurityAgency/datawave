package nsa.datawave.webservice.query.metric;

import java.text.NumberFormat;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import nsa.datawave.webservice.HtmlProvider;
import nsa.datawave.webservice.result.BaseResponse;

@XmlRootElement(name = "QueryMetricsSummaryHtmlResponse")
@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class QueryMetricsSummaryHtmlResponse extends QueryMetricsSummaryResponse implements HtmlProvider {
    
    private static final long serialVersionUID = 1L;
    private static final String TITLE = "Query Metrics Summary", EMPTY = "";
    private static final String START_CELL = "<td>", END_CELL = "</td>";
    
    @Override
    public String getTitle() {
        return TITLE;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see nsa.datawave.webservice.HtmlProvider#getPageHeader()
     */
    @Override
    public String getPageHeader() {
        return getTitle();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see nsa.datawave.webservice.HtmlProvider#getHeadContent()
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
        
        builder.append("<tr><td>1 hour</td>");
        builder.append(START_CELL).append(formatter.format(hour1.getQueryCount())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(hour1.getTotalPages())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(hour1.getTotalPageResultSize())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(hour1.getMinPageResultSize())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(hour1.getMaxPageResultSize())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(hour1.getAvgPageResultSize())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(hour1.getTotalPageResponseTime())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(hour1.getMinPageResponseTime())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(hour1.getMaxPageResponseTime())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(hour1.getAvgPageResponseTime())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(hour1.getAverageResultsPerSecond())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(hour1.getAveragePagesPerSecond())).append(END_CELL);
        builder.append("</tr>\n");
        
        builder.append("<tr class=\"highlight\"><td>6 hours</td>");
        builder.append(START_CELL).append(formatter.format(hour6.getQueryCount())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(hour6.getTotalPages())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(hour6.getTotalPageResultSize())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(hour6.getMinPageResultSize())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(hour6.getMaxPageResultSize())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(hour6.getAvgPageResultSize())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(hour6.getTotalPageResponseTime())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(hour6.getMinPageResponseTime())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(hour6.getMaxPageResponseTime())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(hour6.getAvgPageResponseTime())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(hour6.getAverageResultsPerSecond())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(hour6.getAveragePagesPerSecond())).append(END_CELL);
        builder.append("</tr>\n");
        
        builder.append("<tr><td>12 hours</td>");
        builder.append(START_CELL).append(formatter.format(hour12.getQueryCount())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(hour12.getTotalPages())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(hour12.getTotalPageResultSize())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(hour12.getMinPageResultSize())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(hour12.getMaxPageResultSize())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(hour12.getAvgPageResultSize())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(hour12.getTotalPageResponseTime())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(hour12.getMinPageResponseTime())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(hour12.getMaxPageResponseTime())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(hour12.getAvgPageResponseTime())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(hour12.getAverageResultsPerSecond())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(hour12.getAveragePagesPerSecond())).append(END_CELL);
        builder.append("</tr>\n");
        
        builder.append("<tr class=\"highlight\"><td>1 day</td>");
        builder.append(START_CELL).append(formatter.format(day1.getQueryCount())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(day1.getTotalPages())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(day1.getTotalPageResultSize())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(day1.getMinPageResultSize())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(day1.getMaxPageResultSize())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(day1.getAvgPageResultSize())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(day1.getTotalPageResponseTime())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(day1.getMinPageResponseTime())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(day1.getMaxPageResponseTime())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(day1.getAvgPageResponseTime())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(day1.getAverageResultsPerSecond())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(day1.getAveragePagesPerSecond())).append(END_CELL);
        builder.append("</tr>\n");
        
        builder.append("<tr><td>7 days</td>");
        builder.append(START_CELL).append(formatter.format(day7.getQueryCount())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(day7.getTotalPages())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(day7.getTotalPageResultSize())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(day7.getMinPageResultSize())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(day7.getMaxPageResultSize())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(day7.getAvgPageResultSize())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(day7.getTotalPageResponseTime())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(day7.getMinPageResponseTime())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(day7.getMaxPageResponseTime())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(day7.getAvgPageResponseTime())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(day7.getAverageResultsPerSecond())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(day7.getAveragePagesPerSecond())).append(END_CELL);
        builder.append("</tr>\n");
        
        builder.append("<tr class=\"highlight\"><td>30 days</td>");
        builder.append(START_CELL).append(formatter.format(day30.getQueryCount())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(day30.getTotalPages())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(day30.getTotalPageResultSize())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(day30.getMinPageResultSize())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(day30.getMaxPageResultSize())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(day30.getAvgPageResultSize())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(day30.getTotalPageResponseTime())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(day30.getMinPageResponseTime())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(day30.getMaxPageResponseTime())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(day30.getAvgPageResponseTime())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(day30.getAverageResultsPerSecond())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(day30.getAveragePagesPerSecond())).append(END_CELL);
        builder.append("</tr>\n");
        
        builder.append("<tr><td>60 days</td>");
        builder.append(START_CELL).append(formatter.format(day60.getQueryCount())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(day60.getTotalPages())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(day60.getTotalPageResultSize())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(day60.getMinPageResultSize())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(day60.getMaxPageResultSize())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(day60.getAvgPageResultSize())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(day60.getTotalPageResponseTime())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(day60.getMinPageResponseTime())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(day60.getMaxPageResponseTime())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(day60.getAvgPageResponseTime())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(day60.getAverageResultsPerSecond())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(day60.getAveragePagesPerSecond())).append(END_CELL);
        builder.append("</tr>\n");
        
        builder.append("<tr class=\"highlight\"><td>90 days</td>");
        builder.append(START_CELL).append(formatter.format(day90.getQueryCount())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(day90.getTotalPages())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(day90.getTotalPageResultSize())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(day90.getMinPageResultSize())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(day90.getMaxPageResultSize())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(day90.getAvgPageResultSize())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(day90.getTotalPageResponseTime())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(day90.getMinPageResponseTime())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(day90.getMaxPageResponseTime())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(day90.getAvgPageResponseTime())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(day90.getAverageResultsPerSecond())).append(END_CELL);
        builder.append(START_CELL).append(formatter.format(day90.getAveragePagesPerSecond())).append(END_CELL);
        builder.append("</tr>\n");
        
        builder.append("</table>");
        
        return builder.toString();
    }
    
}
