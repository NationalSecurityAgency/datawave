package datawave.webservice.response.objects;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlTransient;

import datawave.webservice.query.result.event.HasColumnVisibility;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class Visibility implements HasColumnVisibility {

    @XmlAttribute(name = "valid")
    private Boolean valid;

    /**
     * The normalized form of {@link #visibility}, set once the visibility has been validated. Not serialized; deployments that present security markings can
     * translate it at the response edge.
     */
    @XmlTransient
    private String columnVisibility;

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

    /**
     * @return the normalized column visibility, or null if the visibility has not been validated
     */
    @Override
    public String getColumnVisibility() {
        return columnVisibility;
    }

    /**
     * @param columnVisibility
     *            the normalized column visibility
     */
    @Override
    public void setColumnVisibility(String columnVisibility) {
        this.columnVisibility = columnVisibility;
    }

}
