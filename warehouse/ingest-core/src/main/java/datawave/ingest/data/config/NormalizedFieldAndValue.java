package datawave.ingest.data.config;

/**
 * Class to hold the normalized field name and value.  One can set a field specific
 * visibility if desired.  If the visibility is not set, then the event visibility
 * will be used.
 * 
 * 
 *
 */
import java.io.UnsupportedEncodingException;
import java.util.Map;

import datawave.ingest.config.IngestConfiguration;
import datawave.ingest.config.IngestConfigurationFactory;

import datawave.ingest.config.MimeDecoder;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.io.Text;

public class NormalizedFieldAndValue extends BaseNormalizedContent implements GroupedNormalizedContentInterface {
    private boolean grouped = false;
    private String group = null;
    private String subGroup = null;
    private String eventFieldName = null;
    private static MimeDecoder mimeDecoder = null;
    
    // cached defaultEventFieldName. This must be reset to null if anything changes that would affect its contents.
    private transient String defaultEventFieldName = null;
    
    public NormalizedFieldAndValue() {
        super();
    }
    
    public NormalizedFieldAndValue(String field, byte[] value) {
        this(field, value, false);
    }
    
    public NormalizedFieldAndValue(String field, byte[] value, boolean expectBinary) {
        super(field, decode(value, expectBinary));
    }
    
    public NormalizedFieldAndValue(String field, String value) {
        super(field, value);
    }
    
    public NormalizedFieldAndValue(String field, String value, Map<String,String> markings) {
        super(field, value, markings);
    }
    
    public NormalizedFieldAndValue(String field, String value, String group, String subGroup) {
        super(field, value);
        setGrouped(true);
        setGroup(group);
        setSubGroup(subGroup);
    }
    
    public NormalizedFieldAndValue(NormalizedContentInterface n) {
        super(n);
        if (n instanceof GroupedNormalizedContentInterface) {
            GroupedNormalizedContentInterface n2 = (GroupedNormalizedContentInterface) n;
            setGrouped(n2.isGrouped());
            setSubGroup(n2.getSubGroup());
            setGroup(n2.getGroup());
            // only set it if the computed event field name is different
            if (!getDefaultEventFieldName().equals(n.getEventFieldName())) {
                setEventFieldName(n.getEventFieldName());
            }
        }
    }
    
    /**
     * This method will convert bytes into a string value. If not expected to be binary, then it will attempt to decode as UTF8. If that fails or is expected to
     * be binary, then it will decode one byte per character.
     * 
     * @param bytes
     *            the bytes to convert
     * @param expectBinary
     *            flag indicating whether to expect binary
     * @return the value
     */
    public static String decode(byte[] bytes, boolean expectBinary) {
        synchronized (NormalizedFieldAndValue.class) {
            if (null == mimeDecoder) {
                IngestConfiguration config = IngestConfigurationFactory.getIngestConfiguration();
                mimeDecoder = config.createMimeDecoder();
            }
        }
        String value = null;
        if (!expectBinary) {
            try {
                value = Text.decode(mimeDecoder.decode(bytes));
            } catch (Exception e) {
                // ok, treat as binary
            }
        }
        if (value == null) {
            try {
                value = new String(bytes, "ISO8859-1");
            } catch (UnsupportedEncodingException uee) {
                // this should never happen, however....
                throw new RuntimeException(uee);
            }
        }
        return value;
    }
    
    public boolean isGrouped() {
        return this.grouped;
    }
    
    public void setGrouped(boolean grouped) {
        hashCode = null;
        defaultEventFieldName = null;
        this.grouped = grouped;
    }
    
    public String getSubGroup() {
        if (grouped) {
            return subGroup;
        }
        return null;
    }
    
    public void setSubGroup(String group) {
        hashCode = null;
        defaultEventFieldName = null;
        this.subGroup = group;
    }
    
    public String getGroup() {
        if (grouped) {
            return group;
        }
        return null;
    }
    
    public void setGroup(String group) {
        hashCode = null;
        defaultEventFieldName = null;
        this.group = group;
    }
    
    public void setIndexedFieldName(String name) {
        hashCode = null;
        defaultEventFieldName = null;
        // if setting the indexed field name explicitly, save off the current field name as the event field name
        if (this.eventFieldName == null) {
            this.eventFieldName = this._fieldName;
        }
        this._fieldName = name;
    }
    
    @Override
    public void setFieldName(String name) {
        defaultEventFieldName = null;
        super.setFieldName(name);
    }
    
    public void setEventFieldName(String name) {
        hashCode = null;
        this.eventFieldName = name;
    }
    
    public String getEventFieldName() {
        if (eventFieldName == null) {
            return getDefaultEventFieldName();
        } else {
            return eventFieldName;
        }
    }
    
    protected String getDefaultEventFieldName() {
        if (defaultEventFieldName == null) {
            if (isGrouped()) {
                StringBuilder builder = new StringBuilder();
                builder.append(getIndexedFieldName());
                if (getGroup() != null) {
                    builder.append('.');
                    builder.append(getGroup().toUpperCase());
                }
                if (getSubGroup() != null) {
                    builder.append('.');
                    builder.append(getSubGroup().toUpperCase());
                }
                defaultEventFieldName = builder.toString();
            } else {
                defaultEventFieldName = _fieldName;
            }
        }
        return defaultEventFieldName;
    }
    
    public int hashCode() {
        if (hashCode == null) {
            hashCode = super.hashCode();
            // if we are not grouped and the event field name is null,
            // then we are logically equal to the super instance, so return its hashCode
            if (grouped || eventFieldName != null) {
                HashCodeBuilder b = new HashCodeBuilder();
                b.append(hashCode).append(eventFieldName).append(grouped).append(subGroup).append(group);
                hashCode = b.toHashCode();
            }
        }
        return hashCode;
    }
    
    public boolean equals(Object o) {
        boolean equal = super.equals(o);
        if (equal) {
            // if the base is equal and we are not grouped and the event field name is null,
            // then we are logically equal
            if ((!grouped) && (eventFieldName == null)) {
                return true;
            }
            if (o instanceof NormalizedFieldAndValue) {
                NormalizedFieldAndValue n = (NormalizedFieldAndValue) o;
                EqualsBuilder b = new EqualsBuilder();
                b.append(eventFieldName, n.eventFieldName);
                b.append(grouped, n.grouped);
                b.append(subGroup, n.subGroup);
                b.append(group, n.group);
                return b.isEquals();
            }
        }
        return false;
    }
    
    public String toString() {
        StringBuilder sb = new StringBuilder(super.toString());
        sb.append(", eventFieldName=").append(this.eventFieldName);
        sb.append(", grouped=").append(this.grouped);
        sb.append(", subGroup=").append(this.subGroup);
        sb.append(", group=").append(this.group);
        return sb.toString();
    }
    
}
