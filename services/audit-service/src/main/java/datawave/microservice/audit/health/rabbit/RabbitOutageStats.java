package datawave.microservice.audit.health.rabbit;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.microservice.audit.health.OutageStats;
import datawave.webservice.common.audit.Auditor;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * An implementation for {@link OutageStats}, which provides useful information about RabbitMQ outages encountered by the audit service.
 * <p>
 * In addition to the start and stop time of the outage, this implementation provides information about the number of missing nodes in the RabbitMQ cluster, and
 * information detailing which exchanges, queues, or bindings were missing or invalid.
 */
public class RabbitOutageStats extends OutageStats {
    private SimpleDateFormat formatter = new SimpleDateFormat(Auditor.ISO_8601_FORMAT_STRING);
    
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
                        + ", missingBindings="
                        + String.join(",", missingBindings.entries().stream().map(x -> x.getKey() + ": " + x.getValue()).collect(Collectors.toList()))
                        + ", invalidExchanges=" + String.join(",", invalidExchanges) + ", invalidQueues=" + String.join(",", invalidQueues)
                        + ", invalidBindings="
                        + String.join(",", invalidBindings.entries().stream().map(x -> x.getKey() + ": " + x.getValue()).collect(Collectors.toList()));
    }
    
    public int getNumNodesMissing() {
        return numNodesMissing;
    }
    
    public void setNumNodesMissing(int numNodesMissing) {
        this.numNodesMissing = numNodesMissing;
    }
    
    public Set<String> getMissingExchanges() {
        return missingExchanges;
    }
    
    public void setMissingExchanges(Set<String> missingExchanges) {
        this.missingExchanges = missingExchanges;
    }
    
    public Set<String> getMissingQueues() {
        return missingQueues;
    }
    
    public void setMissingQueues(Set<String> missingQueues) {
        this.missingQueues = missingQueues;
    }
    
    public Multimap<String,String> getMissingBindings() {
        return missingBindings;
    }
    
    public void setMissingBindings(Multimap<String,String> missingBindings) {
        this.missingBindings = missingBindings;
    }
    
    public Set<String> getInvalidExchanges() {
        return invalidExchanges;
    }
    
    public void setInvalidExchanges(Set<String> invalidExchanges) {
        this.invalidExchanges = invalidExchanges;
    }
    
    public Set<String> getInvalidQueues() {
        return invalidQueues;
    }
    
    public void setInvalidQueues(Set<String> invalidQueues) {
        this.invalidQueues = invalidQueues;
    }
    
    public Multimap<String,String> getInvalidBindings() {
        return invalidBindings;
    }
    
    public void setInvalidBindings(Multimap<String,String> invalidBindings) {
        this.invalidBindings = invalidBindings;
    }
}
