package datawave.attribute.pointer;

import org.apache.accumulo.core.data.Key;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({@JsonSubTypes.Type(value = ViewDataPointer.class, name = "dView")})
public interface DataPointer {
    Key get();
}
