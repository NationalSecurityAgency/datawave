package datawave.marking;

import static com.google.common.base.Charsets.UTF_8;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;

import org.apache.accumulo.core.security.ColumnVisibility;

import com.google.common.base.Preconditions;

@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class ColumnVisibilitySecurityMarking implements SecurityMarking {
    
    public static final String VISIBILITY_MARKING = "columnVisibility";
    
    @XmlAttribute(name = "columnVisibility")
    private String columnVisibility = null;
    
    @Override
    public void validate(Map<String,List<String>> parameters) throws IllegalArgumentException {
        List<String> values = parameters.get(VISIBILITY_MARKING);
        if (null == values) {
            throw new IllegalArgumentException("Required parameter " + VISIBILITY_MARKING + " not found");
        }
        if (values.isEmpty() || values.size() > 1) {
            throw new IllegalArgumentException("Required parameter " + VISIBILITY_MARKING + " only accepts one value");
        }
        columnVisibility = values.get(0);
        Preconditions.checkNotNull(columnVisibility);
    }
    
    public String getColumnVisibility() {
        return toString();
    }
    
    public void setColumnVisibility(String columnVisibility) {
        this.columnVisibility = columnVisibility;
    }
    
    @Override
    public ColumnVisibility toColumnVisibility() {
        return new ColumnVisibility(columnVisibility);
    }
    
    @Override
    public int hashCode() {
        return this.columnVisibility == null ? 0 : this.columnVisibility.hashCode();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (null == obj) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (obj instanceof ColumnVisibilitySecurityMarking) {
            ColumnVisibilitySecurityMarking other = (ColumnVisibilitySecurityMarking) obj;
            if (this.columnVisibility != null && other.columnVisibility == null) {
                return false;
            }
            if (this.columnVisibility == null && other.columnVisibility != null) {
                return false;
            }
            if (this.columnVisibility == null && other.columnVisibility == null) {
                return true;
            }
            if (this.columnVisibility.equals(other.columnVisibility)) {
                return true;
            } else {
                return false;
            }
        }
        return false;
    }
    
    @Override
    public String toString() {
        return toColumnVisibilityString();
    }
    
    @Override
    public String toColumnVisibilityString() {
        if (null == this.columnVisibility) {
            return null;
        }
        return new String(toColumnVisibility().flatten(), UTF_8);
    }
    
    public void clear() {
        this.columnVisibility = null;
    }
    
    @Override
    public Map<String,String> toMap() {
        return Collections.singletonMap(VISIBILITY_MARKING, this.columnVisibility);
    }
    
    @Override
    public void fromMap(Map<String,String> map) {
        this.columnVisibility = map.get(VISIBILITY_MARKING);
    }
    
    /**
     * Turn this set of markings into a serializable string
     * 
     * @return String
     */
    @Override
    public String mapToString() {
        return MarkingFunctions.Encoding.toString(toMap());
    }
    
    /**
     * Fill this security markings given an encoded string
     * 
     * @param encodedMarkings
     */
    @Override
    public void fromString(String encodedMarkings) {
        Map<String,String> markings = MarkingFunctions.Encoding.fromString(encodedMarkings);
        this.columnVisibility = markings.get(VISIBILITY_MARKING);
        Preconditions.checkNotNull(columnVisibility);
    }
    
}
