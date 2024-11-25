package datawave.microservice.querymetric;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;

@XmlAccessorType(XmlAccessType.NONE)
public class QueryMetricUpdate<T extends BaseQueryMetric> implements Serializable {
    private static final long serialVersionUID = -8110489889196430944L;
    
    @XmlElement
    protected T metric;
    
    @XmlElement
    protected QueryMetricType metricType;
    
    /* constructor for deserializing JSON messages */
    public QueryMetricUpdate() {
        
    }
    
    public QueryMetricUpdate(T metric, QueryMetricType metricType) {
        this.metric = metric;
        this.metricType = metricType;
    }
    
    public QueryMetricUpdate(T metric) {
        this(metric, QueryMetricType.COMPLETE);
    }
    
    public void setMetric(T metric) {
        this.metric = metric;
    }
    
    public T getMetric() {
        return metric;
    }
    
    public void setMetricType(QueryMetricType metricType) {
        this.metricType = metricType;
    }
    
    public QueryMetricType getMetricType() {
        return metricType;
    }
}
