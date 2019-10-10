package datawave.webservice.response.objects;

import datawave.webservice.query.util.OptionallyEncodedString;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class AuthorizationFailure {
    
    @XmlElement
    private OptionallyEncodedString tableId = null;
    
    @XmlElement
    private OptionallyEncodedString endRow = null;
    
    @XmlElement
    private OptionallyEncodedString prevEndRow = null;
    
    public OptionallyEncodedString getTableId() {
        return tableId;
    }
    
    public OptionallyEncodedString getEndRow() {
        return endRow;
    }
    
    public OptionallyEncodedString getPrevEndRow() {
        return prevEndRow;
    }
    
    public void setTableId(OptionallyEncodedString tableId) {
        this.tableId = tableId;
    }
    
    public void setEndRow(OptionallyEncodedString endRow) {
        this.endRow = endRow;
    }
    
    public void setPrevEndRow(OptionallyEncodedString prevEndRow) {
        this.prevEndRow = prevEndRow;
    }
}
