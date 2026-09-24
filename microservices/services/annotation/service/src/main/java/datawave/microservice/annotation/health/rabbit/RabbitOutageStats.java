package datawave.microservice.annotation.health.rabbit;

import static datawave.microservice.annotation.writers.AnnotationWriter.ISO_8601_FORMAT_STRING;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.microservice.annotation.health.OutageStats;
import lombok.Getter;
import lombok.Setter;

/**
 * An implementation for {@link OutageStats}, which provides useful information about RabbitMQ outages encountered by the annotation service.
 * <p>
 * In addition to the start and stop time of the outage, this implementation provides information about the number of missing nodes in the RabbitMQ cluster, and
 * information detailing which exchanges, queues, or bindings were missing or invalid.
 */
@Getter
@Setter
public class RabbitOutageStats extends OutageStats {
    private final SimpleDateFormat formatter = new SimpleDateFormat(ISO_8601_FORMAT_STRING);

    protected int numNodesMissing = 0;

    protected Set<String> missingExchanges = new HashSet<>();
    protected Set<String> missingQueues = new HashSet<>();
    protected Multimap<String,String> missingBindings = HashMultimap.create();

    protected Set<String> invalidExchanges = new HashSet<>();
    protected Set<String> invalidQueues = new HashSet<>();
    protected Multimap<String,String> invalidBindings = HashMultimap.create();

    public RabbitOutageStats(Date startDate) {
        super(startDate);
    }

    /**
     * Collects the applicable RabbitMQ outage fields into a map.
     *
     * @return A map representation of the RabbitOutageStats
     */
    @Override
    public Map<String,Object> getOutageParams() {
        Map<String,Object> statsMap = new LinkedHashMap<>();
        statsMap.put("startDate", formatter.format(startDate));
        statsMap.put("stopDate", (stopDate != null) ? formatter.format(stopDate) : "current");

        if (numNodesMissing > 0)
            statsMap.put("numNodesMissing", numNodesMissing);

        if (!missingExchanges.isEmpty())
            statsMap.put("missingExchanges", missingExchanges);

        if (!missingQueues.isEmpty())
            statsMap.put("missingQueues", missingQueues);

        if (!missingBindings.isEmpty())
            statsMap.put("missingBindings", missingBindings);

        if (!invalidExchanges.isEmpty())
            statsMap.put("invalidExchanges", invalidExchanges);

        if (!invalidQueues.isEmpty())
            statsMap.put("invalidQueues", invalidQueues);

        if (!invalidBindings.isEmpty())
            statsMap.put("invalidBindings", invalidBindings);
        return statsMap;
    }

    @Override
    public String toString() {
        return "startDate=" + formatter.format(startDate) + ", stopDate=" + ((stopDate != null) ? formatter.format(stopDate) : "current") + ", numNodesMissing="
                        + numNodesMissing + ", missingExchanges=" + String.join(",", missingExchanges) + ", missingQueues=" + String.join(",", missingQueues)
                        + ", missingBindings=" + missingBindings.entries().stream().map(x -> x.getKey() + ": " + x.getValue()).collect(Collectors.joining(","))
                        + ", invalidExchanges=" + String.join(",", invalidExchanges) + ", invalidQueues=" + String.join(",", invalidQueues)
                        + ", invalidBindings=" + invalidBindings.entries().stream().map(x -> x.getKey() + ": " + x.getValue()).collect(Collectors.joining(","));
    }
}
