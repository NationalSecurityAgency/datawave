package datawave.webservice.response.objects;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlTransient;

import datawave.marking.Markings;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class Visibility {

    @XmlAttribute(name = "valid")
    private Boolean valid;

    @XmlTransient
    private Markings<?> markings;

    @XmlAttribute(name = "visibility")
    private String visibility;

    public Boolean getValid() {
        return valid;
    }

    public void setValid(Boolean valid) {
        this.valid = valid;
    }

    public String getVisibility() {
        return visibility;
    }

    public void setVisibility(String visibility) {
        this.visibility = visibility;
    }

    public Markings<?> getMarkings() {
        return markings;
    }

    public void setMarkings(Markings<?> markings) {
        this.markings = markings;
    }

}
