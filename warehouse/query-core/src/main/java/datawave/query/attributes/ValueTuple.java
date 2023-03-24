package datawave.query.attributes;

import com.google.common.base.Objects;
import datawave.data.type.Type;
import datawave.query.util.Tuple3;
import org.apache.commons.lang.builder.CompareToBuilder;

import java.util.Collection;

/**
 * 
 */
public class ValueTuple extends Tuple3<String,Object,Object> implements Comparable<ValueTuple> {
    
    private Attribute<?> source = null;
    
    /**
     * @param fieldname
     *            the field name
     * @param value
     *            the value
     * @param normalizedValue
     *            the normalized value
     * @param source
     *            the source
     */
    public ValueTuple(String fieldname, Object value, Object normalizedValue, Attribute<?> source) {
        super(fieldname, value, normalizedValue);
        this.source = source;
    }
    
    public ValueTuple(Collection<String> fieldnames, Object value, Object normalizedValue, Attribute<?> source) {
        this((fieldnames != null && !fieldnames.isEmpty()) ? fieldnames.iterator().next() : "", value, normalizedValue, source);
    }
    
    public String getFieldName() {
        return first();
    }
    
    public Object getValue() {
        return second();
    }
    
    public Object getNormalizedValue() {
        return third();
    }
    
    public Attribute<?> getSource() {
        return source;
    }
    
    public static Object getValue(Object value) {
        if (value instanceof ValueTuple) {
            return ((ValueTuple) value).getValue();
        }
        return value;
    }
    
    public static Object getNormalizedValue(Object value) {
        if (value instanceof ValueTuple) {
            return ((ValueTuple) value).getNormalizedValue();
        } else if (value instanceof Type<?>) {
            return ((Type<?>) value).getNormalizedValue();
        }
        return value;
    }
    
    public static String getStringValue(Object value) {
        if (value instanceof ValueTuple) {
            Object o = ((ValueTuple) value).getValue();
            return (o == null ? null : o.toString());
        }
        return (value == null ? null : value.toString());
    }
    
    public static String getNormalizedStringValue(Object value) {
        if (value instanceof ValueTuple) {
            Object o = ((ValueTuple) value).getNormalizedValue();
            return (o == null ? null : o.toString());
        }
        return (value == null ? null : value.toString());
    }
    
    public static String getFieldName(Object value) {
        if (value instanceof ValueTuple) {
            Object o = ((ValueTuple) value).getFieldName();
            return (o == null ? null : o.toString());
        }
        return (value == null ? null : value.toString());
    }
    
    public static ValueTuple toValueTuple(Object value) {
        if (value instanceof ValueTuple) {
            return ((ValueTuple) value);
        } else {
            return new ValueTuple(value.toString(), value, value, null);
        }
    }
    
    @Override
    public boolean equals(Object o) {
        return (o instanceof ValueTuple) && super.equals(o) && Objects.equal(source, ((ValueTuple) o).source);
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(super.hashCode(), source);
    }
    
    @Override
    public int compareTo(ValueTuple o) {
        CompareToBuilder builder = new CompareToBuilder().append(first(), o.first());
        
        // for the values, lets compare the string variants as the actual values could be of differing types
        builder.append(stringOf(second()), stringOf(o.second())).append(stringOf(third()), stringOf(o.third()));
        
        // for the source, we cannot append(getSource, o.getSource()) because they may be different attribute types
        // and as a result the underlying compareTo method will throw an exception.
        if (getSource() == null || o.getSource() == null) {
            // safe in this case because one of them is null
            builder.append(getSource(), o.getSource());
        } else {
            // otherwise
            builder.append(getSource().getMetadata(), o.getSource().getMetadata()).append(stringOf(getSource().getData()), stringOf(o.getSource().getData()));
        }
        return builder.toComparison();
    }
    
    public String stringOf(Object x) {
        return x == null ? null : x.toString();
    }
}
