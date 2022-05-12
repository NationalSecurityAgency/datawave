package datawave.services.query.configuration;

import org.apache.accumulo.core.client.IteratorSetting;

import java.io.DataInput;
import java.io.IOException;
import java.io.Serializable;

//@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
public class SerializableIteratorSetting extends IteratorSetting implements Serializable {
    /**
     * provide a default constructor for json deserialization
     */
    public SerializableIteratorSetting() {
        super(1000, "default", "class");
    }
    
    public SerializableIteratorSetting(IteratorSetting other) {
        super(other.getPriority(), other.getName(), other.getIteratorClass(), other.getOptions());
    }
    
    public SerializableIteratorSetting(DataInput din) throws IOException {
        super(din);
    }
    
}
