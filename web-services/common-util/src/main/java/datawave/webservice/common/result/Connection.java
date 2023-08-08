package datawave.webservice.common.result;

import java.io.Serializable;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.commons.lang.builder.CompareToBuilder;

import com.google.common.base.Objects;

@XmlRootElement(name = "Connection")
@XmlAccessorType(XmlAccessType.NONE)
public class Connection implements Serializable, Comparable<Connection> {

    private static final long serialVersionUID = 1L;

    @XmlAttribute
    private String state = null;

    @XmlAttribute
    private Long timeInState = null;

    @XmlAttribute
    private String requestLocation = null;

    @XmlElement(name = "Property")
    private Set<ConnectionProperty> connectionProperty = new TreeSet<>();

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public Set<ConnectionProperty> getConnectionProperties() {
        return connectionProperty;
    }

    public TreeMap<String,String> getConnectionPropertiesAsMap() {
        TreeMap<String,String> m = new TreeMap<>();
        for (ConnectionProperty p : connectionProperty) {
            m.put(p.getName(), p.getValue());
        }
        return m;
    }

    public void setConnectionProperties(Set<ConnectionProperty> connectionProperties) {
        this.connectionProperty = connectionProperties;
    }

    public void addProperty(String name, String value) {
        connectionProperty.add(new ConnectionProperty(name, value));
    }

    public Long getTimeInState() {
        return timeInState;
    }

    public void setTimeInState(Long timeInState) {
        this.timeInState = timeInState;
    }

    @Override
    public int compareTo(Connection c) {
        CompareToBuilder b = new CompareToBuilder();
        b.append(this.state, c.state);
        b.append(c.timeInState, this.timeInState);
        return b.toComparison();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Connection)) {
            return false;
        }
        Connection other = (Connection) obj;
        return Objects.equal(this.state, other.state) && Objects.equal(this.timeInState, other.timeInState);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(this.state, this.timeInState);
    }

    public String getRequestLocation() {
        return requestLocation;
    }

    public void setRequestLocation(String requestLocation) {
        this.requestLocation = requestLocation;
    }

}
