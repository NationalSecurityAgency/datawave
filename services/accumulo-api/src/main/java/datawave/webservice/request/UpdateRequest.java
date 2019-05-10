package datawave.webservice.request;

import datawave.webservice.request.objects.ReferencedValue;
import datawave.webservice.request.objects.TableUpdate;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;

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
