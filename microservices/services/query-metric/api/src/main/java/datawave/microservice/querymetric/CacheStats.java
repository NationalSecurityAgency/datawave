package datawave.microservice.querymetric;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

@XmlRootElement(name = "CacheStats")
@XmlAccessorType(XmlAccessType.NONE)
@XmlType(propOrder = {"serviceStats", "incomingQueryMetrics"})
public class CacheStats implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    @XmlAttribute
    private String host;
    
    @XmlAttribute
    private String memberUuid;
    
    @XmlElement(name = "serviceStats")
    @XmlJavaTypeAdapter(StringMapAdapter.class)
    private Map<String,String> serviceStats = new HashMap<>();
    
    @XmlElement(name = "incomingQueryMetrics")
    @XmlJavaTypeAdapter(StringMapAdapter.class)
    private Map<String,String> incomingQueryMetrics = new HashMap<>();
    
    public CacheStats() {
        
    }
    
    public void setHost(String host) {
        this.host = host;
    }
    
    public String getHost() {
        return host;
    }
    
    public void setMemberUuid(String memberUuid) {
        this.memberUuid = memberUuid;
    }
    
    public String getMemberUuid() {
        return memberUuid;
    }
    
    public void setIncomingQueryMetrics(Map<String,String> stats) {
        this.incomingQueryMetrics = stats;
    }
    
    public Map<String,String> getIncomingQueryMetrics() {
        return incomingQueryMetrics;
    }
    
    public void setServiceStats(Map<String,String> serviceStats) {
        this.serviceStats = serviceStats;
    }
    
    public Map<String,String> getServiceStats() {
        return serviceStats;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        CacheStats that = (CacheStats) o;
        return host.equals(that.host) && memberUuid.equals(that.memberUuid) && incomingQueryMetrics.equals(that.incomingQueryMetrics)
                        && serviceStats.equals(that.serviceStats);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(host, memberUuid, incomingQueryMetrics, serviceStats);
    }
}
