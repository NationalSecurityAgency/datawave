package datawave.webservice.modification;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlSeeAlso;
import javax.xml.bind.annotation.adapters.XmlAdapter;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
@XmlSeeAlso({DefaultModificationRequest.class, DefaultUUIDModificationRequest.class})
public class ModificationRequestBase implements Serializable {
    
    private static String COLUMN_VISIBILITY = "columnVisibility";
    
    private static final long serialVersionUID = 1L;
    
    @XmlEnum(String.class)
    public enum MODE {
        INSERT, UPDATE, DELETE
    }
    
    @XmlElement(name = "mode", required = true)
    public MODE mode = null;
    
    public MODE getMode() {
        return mode;
    }
    
    public void setMode(MODE mode) {
        this.mode = mode;
    }
    
    /**
     * Cannot use a setMode(String mode) method as that causes deserialization issues with jackson library.
     * 
     * @param mode
     *            - name of the mode to assign
     */
    public void setModeFromString(String mode) {
        this.mode = MODE.valueOf(mode);
    }
    
    @Override
    public String toString() {
        ToStringBuilder tsb = new ToStringBuilder(this);
        tsb.append("mode", mode);
        return tsb.toString();
    }
    
    public Map<String,List<String>> toMap() {
        MultiValueMap<String,String> p = new LinkedMultiValueMap<>();
        if (this.mode != null) {
            p.set("mode", this.mode.name());
        }
        return p;
    }
    
    public static class DefaultFieldMarkingsAdapter extends XmlAdapter<DefaultFieldMarking,Map<String,String>> {
        
        @Override
        public Map<String,String> unmarshal(DefaultFieldMarking value) throws Exception {
            HashMap<String,String> fieldMarkings = new HashMap<String,String>();
            fieldMarkings.put(COLUMN_VISIBILITY, value.fieldColumnVisibility);
            return fieldMarkings;
        }
        
        @Override
        public DefaultFieldMarking marshal(Map<String,String> map) throws Exception {
            DefaultFieldMarking fieldMarking = new DefaultFieldMarking();
            fieldMarking.fieldColumnVisibility = map.get(COLUMN_VISIBILITY);
            return fieldMarking;
        }
    }
    
    public static class DefaultFieldMarking {
        @XmlElement(name = "columnVisibility", required = true)
        public String fieldColumnVisibility;;
    }
}
