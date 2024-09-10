package datawave.webservice.modification;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import com.google.common.collect.Maps;

/**
 * Object used as the input to the MutableMetadataHandler service deployed as part of the Modification Service. This object contains the information necessary
 * to INSERT, UPDATE, or DELETE entries from the sharded event table. See the MutableMetadataHandler javadoc for examples on what information to provide for
 * each type of request.
 */
@XmlRootElement(name = "DefaultModificationRequest")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class DefaultModificationRequest extends ModificationRequestBase implements Serializable {

    private static final long serialVersionUID = 2L;

    @XmlElementWrapper(name = "Events", required = true)
    @XmlElement(name = "Event", required = true)
    protected List<EventIdentifier> events = null;
    @XmlElement(name = "fieldName", required = true)
    protected String fieldName = null;
    @XmlElement(name = "fieldValue", required = true)
    protected String fieldValue = null;
    @XmlElement(name = "fieldMarkings")
    @XmlJavaTypeAdapter(DefaultFieldMarkingsAdapter.class)
    private Map<String,String> fieldMarkings;
    @XmlElement(name = "columnVisibility")
    protected String columnVisibility = null;
    @XmlElement(name = "oldFieldValue")
    protected String oldFieldValue = null;
    @XmlElement(name = "oldFieldMarkings")
    @XmlJavaTypeAdapter(DefaultFieldMarkingsAdapter.class)
    private Map<String,String> oldFieldMarkings;
    @XmlElement(name = "oldColumnVisibility")
    protected String oldColumnVisibility = null;

    public List<EventIdentifier> getEvents() {
        return events;
    }

    public void setEvents(List<EventIdentifier> events) {
        this.events = events;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getFieldValue() {
        return fieldValue;
    }

    public void setFieldValue(String fieldValue) {
        this.fieldValue = fieldValue;
    }

    public Map<String,String> getFieldMarkings() {
        if (this.fieldMarkings == null)
            this.fieldMarkings = Maps.newHashMap();
        return Maps.newHashMap(fieldMarkings);
    }

    public void setFieldMarkings(Map<String,String> fieldMarkings) {
        this.fieldMarkings = (fieldMarkings == null ? new HashMap<String,String>() : new HashMap<String,String>(fieldMarkings));
    }

    public Map<String,String> getOldFieldMarkings() {
        if (this.oldFieldMarkings == null)
            this.oldFieldMarkings = Maps.newHashMap();
        return Maps.newHashMap(oldFieldMarkings);
    }

    public void setOldFieldMarkings(Map<String,String> oldFieldMarkings) {
        this.oldFieldMarkings = (oldFieldMarkings == null ? new HashMap<String,String>() : new HashMap<String,String>(oldFieldMarkings));
    }

    public String getOldFieldValue() {
        return oldFieldValue;
    }

    public void setOldFieldValue(String oldFieldValue) {
        this.oldFieldValue = oldFieldValue;
    }

    public String getColumnVisibility() {
        return columnVisibility;
    }

    public void setColumnVisibility(String columnVisibility) {
        this.columnVisibility = columnVisibility;
    }

    public String getOldColumnVisibility() {
        return oldColumnVisibility;
    }

    public void setOldColumnVisibility(String oldColumnVisibility) {
        this.oldColumnVisibility = oldColumnVisibility;
    }

    @Override
    public String toString() {
        ToStringBuilder tsb = new ToStringBuilder(this);
        tsb.append("Events", events);
        tsb.append("mode", mode);
        tsb.append("fieldName", fieldName);
        tsb.append("fieldValue", fieldValue);
        tsb.append("fieldMarkings", fieldMarkings);
        tsb.append("columnVisibility", columnVisibility);
        tsb.append("oldFieldValue", oldFieldValue);
        tsb.append("oldFieldMarkings", oldFieldMarkings);
        tsb.append("oldColumnVisibility", oldColumnVisibility);
        return tsb.toString();
    }

    @Override
    public Map<String,List<String>> toMap() {
        MultiValueMap<String,String> p = new LinkedMultiValueMap<>();
        p.putAll(super.toMap());
        if (this.events != null) {
            for (EventIdentifier e : events) {
                p.add("Events", e.toString());
            }
        }
        if (this.fieldName != null) {
            p.add("fieldName", fieldName);
        }
        if (this.fieldValue != null) {
            p.add("fieldValue", fieldValue);
        }
        if (this.fieldMarkings != null) {
            for (Entry<String,String> e : fieldMarkings.entrySet()) {
                p.add("fieldMarkings", e.getKey() + ":" + e.getValue());
            }
        }
        if (this.columnVisibility != null) {
            p.add("columnVisibility", columnVisibility);
        }
        if (this.oldFieldValue != null) {
            p.add("oldFieldValue", oldFieldValue);
        }
        if (this.oldFieldMarkings != null) {
            for (Entry<String,String> e : oldFieldMarkings.entrySet()) {
                p.add("oldFieldMarkings", e.getKey() + ":" + e.getValue());
            }
        }
        if (this.oldColumnVisibility != null) {
            p.add("oldColumnVisibility", oldColumnVisibility);
        }

        return p;
    }
}
