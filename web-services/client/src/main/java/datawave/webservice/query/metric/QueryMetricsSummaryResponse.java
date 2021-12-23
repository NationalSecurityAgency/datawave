package datawave.webservice.query.metric;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import datawave.webservice.result.BaseResponse;

@XmlRootElement(name = "QueryMetricsSummaryResponse")
@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class QueryMetricsSummaryResponse extends BaseResponse {
    
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
    
}
