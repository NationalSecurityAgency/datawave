package datawave.microservice.annotation.health;

import java.util.Date;
import java.util.Map;

import lombok.Getter;
import lombok.Setter;

/**
 * This is the base representation of an infrastructure outage event. A specific implementation should be provided for each infrastructure type that we support
 * (e.g. RabbitMQ, or eventually Kafka)
 */
@Getter
@Setter
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
}
