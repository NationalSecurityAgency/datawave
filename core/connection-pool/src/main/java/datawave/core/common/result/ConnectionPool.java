package datawave.core.common.result;

import java.io.Serializable;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

@XmlRootElement
@XmlAccessorType(XmlAccessType.NONE)
public class ConnectionPool implements Serializable, Comparable<ConnectionPool> {

    private static final long serialVersionUID = 2L;

    public enum Priority {
        ADMIN, HIGH, NORMAL, LOW
    }

    @XmlAttribute
    private String poolName = null;

    @XmlAttribute
    private String priority = null;

    @XmlElement(name = "NumIdle")
    private Integer numIdle = null;

    @XmlElement(name = "NumActive")
    private Integer numActive = null;

    @XmlElement(name = "MaxIdle")
    private Integer maxIdle = null;

    @XmlElement(name = "MaxActive")
    private Integer maxActive = null;

    @XmlElement(name = "NumWaiting")
    private Integer numWaiting = null;

    @XmlElementWrapper(name = "ConnectionRequests")
    @XmlElement(name = "Connection")
    private List<Connection> connectionRequests = null;

    public Integer getNumIdle() {
        return numIdle;
    }

    public void setNumIdle(Integer numIdle) {
        this.numIdle = numIdle;
    }

    public Integer getNumActive() {
        return numActive;
    }

    public void setNumActive(Integer numActive) {
        this.numActive = numActive;
    }

    public Integer getMaxIdle() {
        return maxIdle;
    }

    public void setMaxIdle(Integer maxIdle) {
        this.maxIdle = maxIdle;
    }

    public Integer getMaxActive() {
        return maxActive;
    }

    public void setMaxActive(Integer maxActive) {
        this.maxActive = maxActive;
    }

    public Integer getNumWaiting() {
        return numWaiting;
    }

    public void setNumWaiting(Integer numWaiting) {
        this.numWaiting = numWaiting;
    }

    public List<Connection> getConnectionRequests() {
        return connectionRequests;
    }

    public void setConnectionRequests(List<Connection> connectionRequests) {
        this.connectionRequests = connectionRequests;
    }

    public String getPriority() {
        return priority;
    }

    public void setPriority(String priority) {
        this.priority = priority;
    }

    public String getPoolName() {
        return poolName;
    }

    public void setPoolName(String poolName) {
        this.poolName = poolName;
    }

    @Override
    public int compareTo(ConnectionPool other) {
        return new CompareToBuilder().append(poolName, other.poolName).append(Priority.valueOf(priority), Priority.valueOf(other.priority)).toComparison();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(poolName).append(Priority.valueOf(priority)).toHashCode();
    }

}
