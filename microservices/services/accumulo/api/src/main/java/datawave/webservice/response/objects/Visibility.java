package datawave.webservice.response.objects;

import jakarta.xml.bind.annotation.XmlAccessOrder;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorOrder;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlAttribute;
import jakarta.xml.bind.annotation.XmlTransient;

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
