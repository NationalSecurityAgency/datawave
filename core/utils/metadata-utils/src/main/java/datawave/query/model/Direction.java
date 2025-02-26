package datawave.query.model;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlEnum;

@XmlEnum(String.class)
public enum Direction implements Serializable {
    
    FORWARD("forward"), REVERSE("reverse");
    
    private String value = null;
    
    private Direction(String value) {
        this.value = value;
    }
    
    public String getValue() {
        return this.value;
    }
    
    public static Direction getDirection(String value) {
        return Direction.valueOf(value.toUpperCase());
    }
    
}
