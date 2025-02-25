package datawave.microservice.querymetric;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import org.springframework.web.servlet.ModelAndView;

import datawave.webservice.query.exception.QueryExceptionType;
import datawave.webservice.result.BaseResponse;

@XmlRootElement(name = "QueryMetricsSummaryResponse")
@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class QueryMetricsSummaryResponse extends BaseResponse {
    
    private static final long serialVersionUID = 1L;
    
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
    @XmlTransient
    protected String header;
    @XmlTransient
    protected String footer;
    
    private String basePath = "/querymetric";
    
    public void setBasePath(String basePath) {
        this.basePath = basePath;
    }
    
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
    
    protected void addSummary(List<Map<String,String>> summaryTableContent, QueryMetricSummary summary, String interval) {
        NumberFormat formatter = NumberFormat.getInstance();
        Map<String,String> summaryData = new HashMap<>();
        
        summaryData.put("interval", interval);
        summaryData.put("queryCount", formatter.format(summary.getQueryCount()));
        summaryData.put("totalPages", formatter.format(summary.getTotalPages()));
        summaryData.put("totalPageResultSize", formatter.format(summary.getTotalPageResultSize()));
        summaryData.put("minPageResultSize", formatter.format(summary.getMinPageResultSize()));
        summaryData.put("maxPageResultSize", formatter.format(summary.getMaxPageResultSize()));
        summaryData.put("avgPageResultSize", formatter.format(summary.getAvgPageResultSize()));
        summaryData.put("totalPageResponseTime", formatter.format(summary.getTotalPageResponseTime()));
        summaryData.put("minPageResponseTime", formatter.format(summary.getMinPageResponseTime()));
        summaryData.put("maxPageResponseTime", formatter.format(summary.getMaxPageResponseTime()));
        summaryData.put("avgPageResponseTime", formatter.format(summary.getAvgPageResponseTime()));
        summaryData.put("avgResultsPerSecond", formatter.format(summary.getAverageResultsPerSecond()));
        summaryData.put("avgPagesPerSecond", formatter.format(summary.getAveragePagesPerSecond()));
        
        summaryTableContent.add(summaryData);
    }
    
    public ModelAndView createModelAndView() {
        List<Map<String,String>> summaryTableContent = new ArrayList<>();
        ModelAndView mav = new ModelAndView();
        
        if (getExceptions() == null || getExceptions().isEmpty()) {
            mav.setViewName("querymetricsummary");
            
            NumberFormat formatter = NumberFormat.getInstance();
            formatter.setGroupingUsed(true);
            formatter.setMaximumFractionDigits(2);
            formatter.setParseIntegerOnly(false);
            
            addSummary(summaryTableContent, hour1, "1 hour");
            addSummary(summaryTableContent, hour6, "6 hours");
            addSummary(summaryTableContent, hour12, "12 hours");
            addSummary(summaryTableContent, day1, "1 day");
            addSummary(summaryTableContent, day7, "7 day");
            addSummary(summaryTableContent, day30, "30 days");
            addSummary(summaryTableContent, day60, "60 days");
            addSummary(summaryTableContent, day90, "90 days");
            addSummary(summaryTableContent, all, "all");
            mav.addObject("summaryTableContent", summaryTableContent);
        } else {
            mav.setViewName("querymetricsummaryexceptions");
            List<QueryExceptionType> exceptions = getExceptions();
            mav.addObject("exceptions", exceptions);
            mav.addObject("schema", QueryExceptionType.getSchema());
        }
        mav.addObject("basePath", basePath);
        mav.addObject("header", header);
        mav.addObject("footer", footer);
        return mav;
    }
    
    public void setHeader(String header) {
        this.header = header;
    }
    
    public void setFooter(String footer) {
        this.footer = footer;
    }
}
