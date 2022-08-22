package datawave.webservice.query.metric;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import java.io.Serializable;

public class QueryMetricMessage implements Serializable {
    
    private static final long serialVersionUID = 1L;
    private QueryMetricHolder metricHolder = null;
    
    public QueryMetricMessage(QueryMetricHolder metricHolder) {
        this.metricHolder = metricHolder;
    }
    
    public static long getSerialversionuid() {
        return serialVersionUID;
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37).append(this.metricHolder).toHashCode();
    }
    
    @Override
    public boolean equals(Object o) {
        if (null == o)
            return false;
        if (!(o instanceof QueryMetricMessage))
            return false;
        QueryMetricMessage other = (QueryMetricMessage) o;
        return new EqualsBuilder().append(this.metricHolder, other.metricHolder).isEquals();
    }
    
    @Override
    public String toString() {
        if (this.metricHolder == null) {
            return null;
        } else {
            return this.metricHolder.toString();
        }
    }
    
    public Message toJMSMessage(Session session) throws JMSException {
        ObjectMessage msg = session.createObjectMessage();
        msg.setObject(this.metricHolder);
        return msg;
    }
    
    public static QueryMetricMessage fromJMSMessage(ObjectMessage msg) throws JMSException {
        Object o = msg.getObject();
        if (o instanceof QueryMetricMessage)
            return (QueryMetricMessage) o;
        else
            throw new IllegalArgumentException("Object is of wrong type: " + o.getClass());
    }
    
    public QueryMetricHolder getMetricHolder() {
        return metricHolder;
    }
    
    public void setMetricHolder(QueryMetricHolder metricHolder) {
        this.metricHolder = metricHolder;
    }
}
