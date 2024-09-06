package datawave.core.common.result;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

import com.google.common.base.Objects;

@XmlRootElement
@XmlAccessorType(XmlAccessType.NONE)
public class ConnectionProperty implements Serializable, Comparable<ConnectionProperty> {

    private static final long serialVersionUID = 1L;

    @XmlAttribute(required = true)
    private String name = null;

    @XmlAttribute
    private String value = null;

    public ConnectionProperty() {

    }

    public ConnectionProperty(String name, String value) {

        this.name = name;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public int compareTo(ConnectionProperty cp) {
        return this.name.compareTo(cp.name);
    }

    @Override
    public boolean equals(Object o) {
        if ((o instanceof ConnectionProperty) == false) {
            return false;
        } else {
            return Objects.equal(this.name, ((ConnectionProperty) o).name);
        }
    }

    @Override
    public int hashCode() {
        return this.name.hashCode();
    }
}
