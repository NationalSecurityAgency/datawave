package datawave.query.attributes;

import datawave.webservice.query.data.ObjectSizeOf;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.log4j.Logger;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import com.google.common.base.Preconditions;

public class FieldValueCardinality implements Comparable<FieldValueCardinality> {

    protected String fieldName;

    protected String lower;

    protected String upper;

    protected ICardinality estimate;

    private static final Logger log = Logger.getLogger(FieldValueCardinality.class);

    protected String myValue = null;

    public FieldValueCardinality() {
        estimate = new HyperLogLogPlus(10);
    }

    public FieldValueCardinality(ICardinality otherEstimate) {
        this.estimate = otherEstimate;
    }

    public long sizeInBytes() {
        long size = 8 + 16 + Attribute.sizeInBytes(fieldName) + Attribute.sizeInBytes(lower) + Attribute.sizeInBytes(upper) + Attribute.sizeInBytes(myValue);
        // 8 is object overhead
        // 16 is 4 object references
        size += ObjectSizeOf.Sizer.getObjectSize(estimate);
        return size;
    }

    @Override
    public int compareTo(FieldValueCardinality other) {
        int cmp;

        if (fieldName == null && other.fieldName == null) {
            cmp = 0;
        } else if (fieldName == null) {
            return -1;
        } else if (other.fieldName == null) {
            return 1;
        } else {
            cmp = fieldName.compareTo(other.fieldName);
        }

        if (cmp == 0) {
            cmp = lower.compareTo(other.lower);
        }

        if (cmp == 0) {
            cmp = upper.compareTo(other.upper);
        }

        return cmp;
    }

    @Override
    public boolean equals(Object o) {
        if (null == o) {
            return false;
        }

        if (o instanceof FieldValueCardinality) {
            return 0 == this.compareTo((FieldValueCardinality) o);
        }

        return false;
    }

    @Override
    public int hashCode() {
        HashCodeBuilder hcb = new HashCodeBuilder(2099, 2129);
        hcb.append(fieldName).append(lower).append(upper).append(super.hashCode());

        return hcb.toHashCode();
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public void setContent(String content) {
        this.lower = content;
        this.upper = content;
    }

    public void setCeiling(String upper) {
        myValue = null;
        this.upper = upper;
    }

    public void setFloor(String lower) {
        myValue = null;
        this.lower = lower;

    }

    public void setDoc(Document doc) {
        Attribute<?> attr = doc.get(Document.DOCKEY_FIELD_NAME);
    }

    public void setDocId(String docId) {
        estimate.offer(docId);
    }

    public String getFieldName() {
        return fieldName;
    }

    public String getFloorValue() {
        return lower;
    }

    public String getCeilingValue() {
        return upper;
    }

    /**
     * Return whether the other field value cardinality is within this one.
     *
     * @param other
     *            the other field value cardinality
     * @return if the cardinality is within this
     */
    public boolean isWithin(FieldValueCardinality other) {
        if (fieldName != null && !fieldName.equals(other.fieldName)) {
            return false;
        }

        return lower.compareTo(other.lower) <= 0 && upper.compareTo(other.upper) >= 0;

    }

    public void merge(FieldValueCardinality card) throws CardinalityMergeException {
        if (fieldName != null && !fieldName.equals(card.fieldName)) {
            throw new UnsupportedOperationException("It is not possible to merge FieldValueCardinalities with different field names");
        }

        Preconditions.checkNotNull(card);
        myValue = null;
        this.estimate = estimate.merge(card.estimate);
    }

    public ICardinality getEstimate() {
        return estimate;
    }

    @Override
    public String toString() {
        if (null == myValue)
            myValue = new StringBuilder().append(fieldName).append("; ").append(lower).append(" -- ").append(upper).append("//").append(estimate.cardinality())
                            .toString();
        return myValue;
    }

}
