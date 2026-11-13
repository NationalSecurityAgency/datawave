package datawave.webservice.request.objects;

import java.util.List;

import jakarta.xml.bind.annotation.XmlAccessOrder;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorOrder;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlAttribute;
import jakarta.xml.bind.annotation.XmlElement;

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
