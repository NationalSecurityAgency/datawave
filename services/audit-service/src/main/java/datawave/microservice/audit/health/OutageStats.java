package datawave.microservice.audit.health;

import java.util.Date;
import java.util.Map;

/**
 * This is the base representation of an infrastructure outage event. A specific implementation should be provided for each infrastructure type that we support
 * (e.g. RabbitMQ, or eventually Kafka)
 */
public abstract class OutageStats implements Comparable<OutageStats> {
    
    protected Date startDate;
    protected Date stopDate;
    
    public OutageStats(Date startDate) {
        this.startDate = startDate;
    }
    
    /**
     * Collects the outage parameters associated with ths outage event.
     * 
     * @return a map representing the stats about the outage
     */
    public abstract Map<String,Object> getOutageParams();
    
    @Override
    public int compareTo(OutageStats o) {
        return startDate.compareTo(o.startDate);
    }
    
    public Date getStartDate() {
        return startDate;
    }
    
    public void setStartDate(Date startDate) {
        this.startDate = startDate;
    }
    
    public Date getStopDate() {
        return stopDate;
    }
    
    public void setStopDate(Date stopDate) {
        this.stopDate = stopDate;
    }
}
