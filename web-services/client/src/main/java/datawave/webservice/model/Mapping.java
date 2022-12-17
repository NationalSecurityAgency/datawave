package datawave.webservice.model;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "Mapping")
@XmlAccessorType(XmlAccessType.NONE)
public class Mapping implements Comparable<Mapping> {
    
    @XmlAttribute(name = "direction", required = true)
    protected Direction direction = null;
    
    @XmlAttribute(name = "modelFieldName", required = true)
    protected String modelFieldName = null;
    
    @XmlAttribute(name = "columnVisibility", required = true)
    protected String columnVisibility = null;
    
    @XmlAttribute(name = "datatype", required = true)
    protected String datatype = null;
    
    public Direction getDirection() {
        return direction;
    }
    
    public void setDirection(Direction direction) {
        this.direction = direction;
    }
    
    public String getModelFieldName() {
        return modelFieldName;
    }
    
    public void setModelFieldName(String modelFieldName) {
        this.modelFieldName = modelFieldName;
    }
    
    public String getColumnVisibility() {
        return columnVisibility;
    }
    
    public void setColumnVisibility(String columnVisibility) {
        this.columnVisibility = columnVisibility;
    }
    
    public String getDatatype() {
        return datatype;
    }
    
    public void setDatatype(String datatype) {
        this.datatype = datatype;
    }
    
    public void appendFields(StringBuilder builder) {}
    
    public String getProjection() {
        return null;
    }
    
    @Override
    public int compareTo(Mapping obj) {
        
        if (obj == null) {
            throw new IllegalArgumentException("can not compare null");
        }
        
        if (obj == this)
            return 0;
        return 1;
    }
}
