package datawave.webservice.response.objects;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;

import org.apache.commons.codec.binary.Base64;

import datawave.marking.MarkingFunctions;
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
    public void setMarkings(Map<String,String> markings) {
        HashMap<String,String> markingMap = new HashMap<>();
        if (markings != null) {
            markingMap.putAll(markings);
        }
        this.markings = markingMap;
        
        setColumnVisibility(markingMap.getOrDefault(MarkingFunctions.Default.COLUMN_VISIBILITY, ""));
    }
    
    @Override
    public Map<String,String> getMarkings() {
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
        if (markings == null) {
            markings = new HashMap<>();
        }
        markings.put(MarkingFunctions.Default.COLUMN_VISIBILITY, this.columnVisibility.getValue().toString());
        
    }
    
    public void setTimestamp(long timestamp) {
        this.timestamp = new TypedValue(timestamp);
    }
}
