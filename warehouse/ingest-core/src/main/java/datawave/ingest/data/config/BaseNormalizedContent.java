package datawave.ingest.data.config;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import datawave.data.type.Type;

/**
 * Base container class implementation of the NormalizedContentInterface. This class is utilized to retain the original and transformed content and labels for a
 * value pair utilized for ingest into Accumulo.
 */
public class BaseNormalizedContent implements NormalizedContentInterface, Cloneable {
    /** The original field label of data. */
    protected String _fieldName = null;

    /** The original field value of data. */
    protected String _eventFieldValue = null;

    /** The transformed field value of data. */
    protected String _indexedFieldValue = null;

    /** The field specific visibility for the field value pair, or null to use the event visibility. */
    protected ColumnVisibility _visibility = null;

    /** The field processing error if any. */
    protected Throwable error = null;

    // a cached hash code. This must be reset to null if anything changes that would affect its contents
    protected transient Integer hashCode = null;

    public BaseNormalizedContent() {

    }

    public BaseNormalizedContent(String field, String value) {
        _fieldName = field;
        _eventFieldValue = value;
        _indexedFieldValue = value;
    }

    public BaseNormalizedContent(String field, String value, ColumnVisibility visibility) {
        this(field, value);
        _visibility = visibility;
    }

    public BaseNormalizedContent(NormalizedContentInterface n) {
        setFieldName(n.getIndexedFieldName());
        setIndexedFieldValue(n.getIndexedFieldValue());
        setEventFieldValue(n.getEventFieldValue());
        setVisibility(n.getVisibility());
        setError(n.getError());
    }

    /**
     * Setter for field label
     *
     * @param fieldName
     *            the value to set
     */
    public void setFieldName(String fieldName) {
        hashCode = null;
        _fieldName = fieldName;
    }

    /**
     * Getter for field label
     *
     * @return the field lavel
     */
    public String getIndexedFieldName() {
        return _fieldName;
    }

    /**
     * Getter for field label
     *
     * @return the field lavel
     */
    public String getEventFieldName() {
        return _fieldName;
    }

    /**
     * Getter for the normalized field value
     *
     * @return the normalized field value
     */
    public String getIndexedFieldValue() {
        return _indexedFieldValue;
    }

    /**
     * Setter for normalized field value
     *
     * @param normalizedFieldValue
     *            the value to set
     */
    public void setIndexedFieldValue(String normalizedFieldValue) {
        hashCode = null;
        _indexedFieldValue = normalizedFieldValue;
    }

    /**
     * Getter for the original field value
     *
     * @return the original field value
     */
    public String getEventFieldValue() {
        return _eventFieldValue;
    }

    public void setEventFieldValue(String originalFieldValue) {
        hashCode = null;
        _eventFieldValue = originalFieldValue;
    }

    @Override
    public ColumnVisibility getVisibility() {
        return _visibility;
    }

    @Override
    public void setVisibility(ColumnVisibility visibility) {
        hashCode = null;
        this._visibility = visibility;
    }

    /**
     * Setter for the processing error
     *
     * @param error
     *            The processing error
     */
    public void setError(Throwable error) {
        hashCode = null;
        this.error = error;
    }

    /**
     * Getter for the processing error
     *
     * @return the processing error
     */
    public Throwable getError() {
        return error;
    }

    public int hashCode() {
        if (hashCode == null) {
            HashCodeBuilder b = new HashCodeBuilder();
            b.append(_fieldName).append(_indexedFieldValue).append(_eventFieldValue);
            b.append(_visibility);
            if (error != null) {
                b.append(error); // we want error in the hashCode only when it is non-null
            }
            hashCode = b.toHashCode();
        }
        return hashCode;
    }

    public boolean equals(Object o) {
        if (o instanceof BaseNormalizedContent) {
            BaseNormalizedContent n = (BaseNormalizedContent) o;
            EqualsBuilder b = new EqualsBuilder();
            b.append(_fieldName, n._fieldName);
            b.append(_indexedFieldValue, n._indexedFieldValue);
            b.append(_eventFieldValue, n._eventFieldValue);
            b.append(_visibility, n._visibility);
            b.append(error, n.error);
            return b.isEquals();
        }
        return false;
    }

    public Object clone() {
        Object clone = null;
        try {
            clone = super.clone();
        } catch (CloneNotSupportedException cnse) {
            // not possible
        }
        return clone;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("fieldName=").append(this._fieldName);
        sb.append(", indexedFieldValue=").append(this._indexedFieldValue);
        sb.append(", eventFieldValue=").append(this._eventFieldValue);
        sb.append(", visibility=").append(this._visibility == null ? null : new String(this._visibility.getExpression(), UTF_8));
        if (null != this.error) {
            sb.append(", error=").append(this.error.getClass().getCanonicalName());
        }
        return sb.toString();
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.ingest.data.config.NormalizedContentInterface#normalize(datawave.data.type.Type)
     */
    @Override
    public void normalize(Type<?> datawaveType) {
        try {
            this.setIndexedFieldValue(datawaveType.normalize(this.getIndexedFieldValue()));
        } catch (Exception e) {
            this.setError(e);
        }
    }
}
