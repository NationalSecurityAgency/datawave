package datawave.webservice.request.objects;

import java.util.List;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;

import datawave.webservice.query.util.OptionallyEncodedString;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class Mutation {
    
    @XmlElement(required = true)
    private OptionallyEncodedString row = null;
    
    @XmlElement(name = "mutationEntry")
    private List<MutationEntry> mutationEntries = null;
    
    public OptionallyEncodedString getRow() {
        return row;
    }
    
    public List<MutationEntry> getMutationEntries() {
        return mutationEntries;
    }
    
    public void setRow(OptionallyEncodedString row) {
        this.row = row;
    }
    
    public void setMutationEntries(List<MutationEntry> mutationEntry) {
        this.mutationEntries = mutationEntry;
    }
}
