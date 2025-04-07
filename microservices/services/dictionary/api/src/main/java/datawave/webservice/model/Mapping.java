package datawave.webservice.model;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;

import org.apache.commons.lang3.builder.CompareToBuilder;

import datawave.query.model.Direction;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@XmlAccessorType(XmlAccessType.NONE)
public class Mapping implements Serializable, Comparable<Mapping> {
    
    private static final long serialVersionUID = 1L;
    
    @XmlAttribute(required = true)
    private String datatype;
    
    @XmlAttribute(required = true)
    private String fieldName;
    
    @XmlAttribute(required = true)
    private String modelFieldName;
    
    @XmlAttribute(required = true)
    private Direction direction;
    
    @XmlAttribute(required = true)
    private String columnVisibility;
    
    @Override
    public int compareTo(Mapping obj) {
        
        if (obj == null) {
            throw new IllegalArgumentException("can not compare null");
        }
        
        if (obj == this)
            return 0;
        
        return new CompareToBuilder().append(datatype, obj.datatype).append(fieldName, obj.fieldName).append(modelFieldName, obj.modelFieldName)
                        .append(direction, obj.direction).append(columnVisibility, obj.columnVisibility).toComparison();
    }
    
}
