package datawave.webservice.request;

import java.util.List;

import jakarta.xml.bind.annotation.XmlAccessOrder;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorOrder;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlRootElement;

import datawave.webservice.request.objects.ReferencedValue;
import datawave.webservice.request.objects.TableUpdate;

@XmlRootElement(name = "UpdateRequest")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class UpdateRequest {

    @XmlElement(name = "referencedValue")
    private List<ReferencedValue> referencedValues = null;

    @XmlElement(name = "tableUpdate")
    private List<TableUpdate> tableUpdates = null;

    public List<ReferencedValue> getReferencedValues() {
        return referencedValues;
    }

    public List<TableUpdate> getTableUpdates() {
        return tableUpdates;
    }

    public void setReferencedValues(List<ReferencedValue> referencedValues) {
        this.referencedValues = referencedValues;
    }

    public void setTableUpdates(List<TableUpdate> tableUpdate) {
        this.tableUpdates = tableUpdate;
    }
}
