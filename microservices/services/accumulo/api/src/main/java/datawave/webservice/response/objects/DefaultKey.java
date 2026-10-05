package datawave.webservice.response.objects;

import static java.nio.charset.StandardCharsets.UTF_8;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;

import org.apache.accumulo.access.AccessExpression;
import org.apache.commons.codec.binary.Base64;

import datawave.marking.AccessExpressionMarkings;
import datawave.marking.Markings;
import datawave.webservice.query.util.TypedValue;

@XmlAccessorType(XmlAccessType.NONE)
public class DefaultKey extends KeyBase {

    @XmlElement(name = "Row")
    private TypedValue row;

    @XmlElement(name = "ColFam")
    private TypedValue colFam;

    @XmlElement(name = "ColQual")
    private TypedValue colQual;

    @XmlElement(name = "ColumnVisibility")
    private TypedValue columnVisibility;

    @XmlElement(name = "Timestamp")
    private TypedValue timestamp;

    public DefaultKey(String row, String colFam, String colQual, String columnVisibility, Long timestamp) {
        this.row = new TypedValue(row);
        this.colFam = new TypedValue(colFam);
        this.colQual = new TypedValue(colQual);
        this.columnVisibility = new TypedValue(columnVisibility);
        this.timestamp = new TypedValue(timestamp);
    }

    public DefaultKey() {}

    @Override
    public String getRow() {
        if (this.row.getType().equals(TypedValue.XSD_STRING) && this.row.isBase64Encoded()) {
            return new String(Base64.decodeBase64(this.row.getValue().toString().getBytes(UTF_8)), UTF_8);
        } else {
            return this.row.getValue().toString();
        }
    }

    @Override
    public String getColFam() {
        if (this.colFam.getType().equals(TypedValue.XSD_STRING) && this.colFam.isBase64Encoded()) {
            return new String(Base64.decodeBase64(this.colFam.getValue().toString().getBytes(UTF_8)), UTF_8);
        } else {
            return this.colFam.getValue().toString();
        }
    }

    @Override
    public String getColQual() {
        if (this.colQual.getType().equals(TypedValue.XSD_STRING) && this.colQual.isBase64Encoded()) {
            return new String(Base64.decodeBase64(this.colQual.getValue().toString().getBytes(UTF_8)), UTF_8);
        } else {
            return this.colQual.getValue().toString();
        }
    }

    public String getColumnVisibility() {
        if (this.columnVisibility.getType().equals(TypedValue.XSD_STRING) && this.columnVisibility.isBase64Encoded()) {
            return new String(Base64.decodeBase64(this.columnVisibility.getValue().toString().getBytes(UTF_8)), UTF_8);
        } else {
            return this.columnVisibility.getValue().toString();
        }
    }

    @Override
    public long getTimestamp() {
        if (this.timestamp.getType().equals(TypedValue.XSD_LONG)) {
            return (Long) this.timestamp.getValue();
        } else {
            return 0L;
        }
    }

    @Override
    public void setMarkings(Markings<?> markings) {
        this.markings = markings;
        AccessExpression ae = markings.toAccessExpression();
        this.columnVisibility = new TypedValue(ae != null ? ae.getExpression() : "");
    }

    @Override
    public Markings<?> getMarkings() {
        return this.markings;
    }

    public void setRow(String row) {
        this.row = new TypedValue(row);
    }

    public void setColFam(String colFam) {
        this.colFam = new TypedValue(colFam);
    }

    public void setColQual(String colQual) {
        this.colQual = new TypedValue(colQual);
    }

    public void setColumnVisibility(String colVis) {
        this.columnVisibility = (colVis == null) ? new TypedValue("") : new TypedValue(colVis);
        String expr = this.columnVisibility.getValue().toString();
        this.markings = AccessExpressionMarkings.builder().columnVisibility(expr).build();
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = new TypedValue(timestamp);
    }
}
