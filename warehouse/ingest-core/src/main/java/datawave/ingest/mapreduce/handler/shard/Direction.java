package datawave.ingest.mapreduce.handler.shard;

import javax.xml.bind.annotation.XmlEnum;
import java.io.Serializable;

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
