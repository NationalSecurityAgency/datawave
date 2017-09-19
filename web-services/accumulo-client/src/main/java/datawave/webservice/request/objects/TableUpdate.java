package datawave.webservice.request.objects;

import java.util.List;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class TableUpdate {
    
    @XmlElement(name = "mutation")
    private List<Mutation> mutations = null;
    
    @XmlAttribute(required = true)
    private String table = null;
    
    public List<Mutation> getMutations() {
        return mutations;
    }
    
    public String getTable() {
        return table;
    }
    
    public void setMutations(List<Mutation> mutation) {
        this.mutations = mutation;
    }
    
    public void setTable(String table) {
        this.table = table;
    }
}
