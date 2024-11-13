package datawave.webservice.results.modification;

import java.io.Serializable;
import java.util.Set;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "MutableFieldListResponse")
@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class MutableFieldListResponse implements Serializable {

    private static final long serialVersionUID = 1L;

    @XmlAttribute(name = "Datatype", required = true)
    private String datatype = null;

    @XmlElementWrapper(name = "MutableFields")
    @XmlElement(name = "Field")
    private Set<String> mutableFields = null;

    public String getDatatype() {
        return datatype;
    }

    public void setDatatype(String datatype) {
        this.datatype = datatype;
    }

    public Set<String> getMutableFields() {
        return mutableFields;
    }

    public void setMutableFields(Set<String> mutableFields) {
        this.mutableFields = mutableFields;
    }

}
