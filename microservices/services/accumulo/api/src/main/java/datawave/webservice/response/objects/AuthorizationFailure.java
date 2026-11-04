package datawave.webservice.response.objects;

import jakarta.xml.bind.annotation.XmlAccessOrder;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorOrder;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;

import datawave.webservice.query.util.OptionallyEncodedString;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class AuthorizationFailure {

    @XmlElement
    private OptionallyEncodedString tableName = null;

    @XmlElement
    private OptionallyEncodedString endRow = null;

    @XmlElement
    private OptionallyEncodedString prevEndRow = null;

    public OptionallyEncodedString getTableName() {
        return tableName;
    }

    public OptionallyEncodedString getEndRow() {
        return endRow;
    }

    public OptionallyEncodedString getPrevEndRow() {
        return prevEndRow;
    }

    public void setTableName(OptionallyEncodedString tableName) {
        this.tableName = tableName;
    }

    public void setEndRow(OptionallyEncodedString endRow) {
        this.endRow = endRow;
    }

    public void setPrevEndRow(OptionallyEncodedString prevEndRow) {
        this.prevEndRow = prevEndRow;
    }
}
