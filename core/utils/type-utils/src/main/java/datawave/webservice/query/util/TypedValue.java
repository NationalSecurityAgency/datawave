package datawave.webservice.query.util;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.xml.bind.DatatypeConverter;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlValue;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import datawave.data.normalizer.DateNormalizer;
import datawave.data.type.DateType;
import datawave.data.type.IpAddressType;
import datawave.data.type.NoOpType;
import datawave.data.type.NumberType;
import datawave.data.type.Type;
import datawave.webservice.query.data.ObjectSizeOf;
import io.protostuff.Input;
import io.protostuff.Message;
import io.protostuff.Output;
import io.protostuff.Schema;

@XmlRootElement
@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class TypedValue implements Serializable, Message<TypedValue> {
    private static final long serialVersionUID = 1987198355354220378L;
    
    public static final String XSD_BOOLEAN = "xs:boolean";
    public static final String XSD_BYTE = "xs:byte";
    public static final String XSD_DATETIME = "xs:dateTime";
    public static final String XSD_DECIMAL = "xs:decimal";
    public static final String XSD_DOUBLE = "xs:double";
    public static final String XSD_FLOAT = "xs:float";
    public static final String XSD_HEXBINARY = "xs:hexBinary";
    public static final String XSD_BASE64BINARY = "xs:base64Binary";
    public static final String XSD_INT = "xs:int";
    public static final String XSD_INTEGER = "xs:integer";
    public static final String XSD_LONG = "xs:long";
    public static final String XSD_SHORT = "xs:short";
    public static final String XSD_STRING = "xs:string";
    public static final String XSD_IPADDRESS = "xs:ipAddress";
    public static final String MAX_UNICODE_STRING = new String(Character.toChars(Character.MAX_CODE_POINT));
    
    @XmlAttribute(required = false)
    private Boolean base64Encoded;
    
    @XmlAttribute
    private String type;
    
    // NOTE: Primitive type info is sometimes lost for Object value using ObjectMapper (de)serialization (i.e. Long becomes Integer, Float becomes
    // Double) so only use the marshalledValue
    @JsonIgnore
    @XmlTransient
    private Object value;
    
    @XmlTransient
    private Class<?> dataType;
    
    @JsonProperty
    @XmlValue
    private String marshalledValue;
    
    public TypedValue() {}
    
    public TypedValue(Object value) {
        setDataType(value.getClass());
        setValue(value);
    }
    
    public long sizeInBytes() {
        // return the approximate overhead of this class
        long size = 28;
        // 8 for the object overhead
        // 20 for the object references
        // all rounded up to the nearest multiple of 8
        size += (base64Encoded == null ? 0 : 16) + sizeInBytes(type) + ObjectSizeOf.Sizer.getObjectSize(value) + sizeInBytes(marshalledValue);
        // note we are ignoring Class object
        return size;
    }
    
    // a helper method to return the size of a string
    protected static long sizeInBytes(String value) {
        if (value == null) {
            return 0;
        } else {
            return 24 + roundUp(12 + (value.length() * 2));
            // 24 for 3 ints, array ref, and object overhead
            // 12 for array overhead
        }
    }
    
    protected static long roundUp(long size) {
        long extra = size % 8;
        if (extra > 0) {
            size = size + 8 - extra;
        }
        return size;
    }
    
    public Object getValue() {
        // NOTE: Proper (de)serialization via ObjectMapper relies on this check to populate value via marshalledValue
        if (null != this.marshalledValue && null == this.value) {
            afterUnmarshal((Unmarshaller) null, null);
        }
        return value;
    }
    
    public void setValue(Object value) {
        this.value = value;
        
        Class<?> clazz = value.getClass();
        if (String.class.equals(clazz)) {
            String string = (String) value;
            // this can happen when a HIT_TERM was created from a Composite field. Remove the composite separator
            if (string.contains(MAX_UNICODE_STRING)) {
                string = string.replaceAll(MAX_UNICODE_STRING, " ");
            }
            setMarshalledStringValue(string);
        } else if (byte[].class.equals(clazz)) {
            this.marshalledValue = DatatypeConverter.printBase64Binary((byte[]) value);
            this.type = XSD_BASE64BINARY;
        } else if (Boolean.class.equals(clazz)) {
            this.marshalledValue = DatatypeConverter.printBoolean((Boolean) value);
            this.type = XSD_BOOLEAN;
        } else if (Byte.class.equals(clazz)) {
            this.marshalledValue = DatatypeConverter.printByte((Byte) value);
            this.type = XSD_BYTE;
        } else if (Date.class.isAssignableFrom(clazz)) {
            Date d = (Date) value;
            DateNormalizer dn = new DateNormalizer();
            this.marshalledValue = DatatypeConverter.printString(dn.parseToString(d));
            this.type = XSD_DATETIME;
        } else if (Calendar.class.isAssignableFrom(clazz)) {
            this.marshalledValue = DatatypeConverter.printDateTime((Calendar) value);
            this.type = XSD_DATETIME;
        } else if (BigDecimal.class.equals(clazz)) {
            this.marshalledValue = DatatypeConverter.printDecimal((BigDecimal) value);
            this.type = XSD_DECIMAL;
        } else if (Number.class.equals(clazz)) {
            this.marshalledValue = DatatypeConverter.printString((String) value);
            this.type = XSD_DECIMAL;
        } else if (Double.class.equals(clazz)) {
            this.marshalledValue = DatatypeConverter.printDouble((Double) value);
            this.type = XSD_DOUBLE;
        } else if (Float.class.equals(clazz)) {
            this.marshalledValue = DatatypeConverter.printFloat((Float) value);
            this.type = XSD_FLOAT;
        } else if (Integer.class.equals(clazz)) {
            this.marshalledValue = DatatypeConverter.printInt((Integer) value);
            this.type = XSD_INT;
        } else if (BigInteger.class.equals(clazz)) {
            this.marshalledValue = DatatypeConverter.printInteger((BigInteger) value);
            this.type = XSD_INTEGER;
        } else if (Long.class.equals(clazz)) {
            this.marshalledValue = DatatypeConverter.printLong((Long) value);
            this.type = XSD_LONG;
        } else if (Short.class.equals(clazz)) {
            this.marshalledValue = DatatypeConverter.printShort((Short) value);
            this.type = XSD_SHORT;
        } else if (IpAddressType.class.equals(clazz)) {
            Type<?> type = (Type<?>) value;
            String valueToDisplay = type.getDelegate().toString();
            this.marshalledValue = DatatypeConverter.printString(valueToDisplay);
            this.type = XSD_IPADDRESS;
        } else if (DateType.class.equals(clazz)) {
            Type<?> type = (Type<?>) value;
            Date d = (Date) type.getDelegate();
            DateNormalizer dn = new DateNormalizer();
            this.marshalledValue = DatatypeConverter.printString(dn.parseToString(d));
            this.type = XSD_DATETIME;
        } else if (NumberType.class.equals(clazz)) {
            NumberType dn = (NumberType) value;
            BigDecimal bd = dn.getDelegate();
            this.marshalledValue = DatatypeConverter.printDecimal(bd);
            this.type = XSD_DECIMAL;
        } else if (clazz.toString().contains("IpV4Address")) {
            this.marshalledValue = DatatypeConverter.printString(value.toString());
            this.type = XSD_IPADDRESS;
        } else if (clazz.toString().contains("IpV6Address")) {
            this.marshalledValue = DatatypeConverter.printString(value.toString());
            this.type = XSD_IPADDRESS;
        } else if (NoOpType.class.equals(clazz)) {
            Type<?> type = (Type<?>) value;
            String valueToDisplay = type.getDelegate().toString();
            setMarshalledStringValue(valueToDisplay);
        } else if (Type.class.isAssignableFrom(clazz)) {
            Type<?> type = (Type<?>) value;
            String valueToDisplay = type.getDelegate().toString();
            valueToDisplay = valueToDisplay.replaceAll(MAX_UNICODE_STRING, "");
            setMarshalledStringValue(valueToDisplay);
        } else if (LinkedHashMap.class.equals(clazz)) {
            // this is a special case when Json serialization is used. The Type objects end up being a map.
            setMarshalledStringValue(String.valueOf(((LinkedHashMap) value).get("delegate")));
        } else {
            throw new IllegalArgumentException("Unhandled class type: " + clazz.getName());
        }
    }
    
    private void setMarshalledStringValue(String string) {
        if (XMLUtil.isValidXML(string)) {
            this.marshalledValue = DatatypeConverter.printString(string);
        } else {
            this.marshalledValue = DatatypeConverter.printBase64Binary(string.getBytes(Charset.forName("UTF-8")));
            base64Encoded = Boolean.TRUE;
        }
        this.type = XSD_STRING;
    }
    
    public String getType() {
        return type;
    }
    
    public void setDataType(Class<?> dataType) {
        this.dataType = dataType;
    }
    
    public Class<?> getDataType() {
        return this.dataType;
    }
    
    public boolean isBase64Encoded() {
        return base64Encoded != null && base64Encoded;
    }
    
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append("TypedValue [base64Encoded=").append(isBase64Encoded());
        buf.append(" type=").append(type);
        buf.append(" marshalledValue=").append(marshalledValue);
        buf.append(" value= ");
        if (null != value)
            buf.append(value).append("] ");
        else
            buf.append("null ]");
        return buf.toString();
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((base64Encoded == null) ? 0 : base64Encoded.hashCode());
        result = prime * result + ((marshalledValue == null) ? 0 : marshalledValue.hashCode());
        result = prime * result + ((type == null) ? 0 : type.hashCode());
        result = prime * result + ((value == null) ? 0 : value.hashCode());
        return result;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        TypedValue other = (TypedValue) obj;
        if (base64Encoded == null) {
            if (other.base64Encoded != null)
                return false;
        } else if (!base64Encoded.equals(other.base64Encoded))
            return false;
        if (marshalledValue == null) {
            if (other.marshalledValue != null)
                return false;
        } else if (!marshalledValue.equals(other.marshalledValue))
            return false;
        if (type == null) {
            if (other.type != null)
                return false;
        } else if (!type.equals(other.type))
            return false;
        if (value == null) {
            if (other.value != null)
                return false;
        } else if (other.value == null) {
            return false;
        } else if (value instanceof Type<?> && other.value instanceof Type<?>) {
            Type<?> thisValue = (Type<?>) this.value;
            Type<?> thatValue = (Type<?>) other.value;
            return thisValue.getDelegate().equals(thatValue.getDelegate());
        } else if (!value.equals(other.value))
            return false;
        return true;
    }
    
    // Method is called by the JAXB marshalling code
    private void afterUnmarshal(Unmarshaller unmarshaller, Object parent) {
        if (XSD_STRING.equals(type)) {
            if (isBase64Encoded()) {
                value = new String(DatatypeConverter.parseBase64Binary(marshalledValue), Charset.forName("UTF-8"));
            } else {
                value = DatatypeConverter.parseString(marshalledValue);
            }
        } else if (XSD_HEXBINARY.equals(type)) {
            value = DatatypeConverter.parseHexBinary(marshalledValue);
        } else if (XSD_BASE64BINARY.equals(type)) {
            value = DatatypeConverter.parseBase64Binary(marshalledValue);
        } else if (XSD_BOOLEAN.equals(type)) {
            value = DatatypeConverter.parseBoolean(marshalledValue);
        } else if (XSD_BYTE.equals(type)) {
            value = DatatypeConverter.parseBoolean(marshalledValue);
        } else if (XSD_DATETIME.equals(type)) {
            value = new DateType(marshalledValue).getDelegate();
        } else if (XSD_DECIMAL.equals(type)) {
            value = DatatypeConverter.parseDecimal(marshalledValue);
        } else if (XSD_DOUBLE.equals(type)) {
            value = DatatypeConverter.parseDouble(marshalledValue);
        } else if (XSD_FLOAT.equals(type)) {
            value = DatatypeConverter.parseFloat(marshalledValue);
        } else if (XSD_INT.equals(type)) {
            value = DatatypeConverter.parseInt(marshalledValue);
        } else if (XSD_INTEGER.equals(type)) {
            value = DatatypeConverter.parseInteger(marshalledValue);
        } else if (XSD_LONG.equals(type)) {
            value = DatatypeConverter.parseLong(marshalledValue);
        } else if (XSD_SHORT.equals(type)) {
            value = DatatypeConverter.parseShort(marshalledValue);
        } else if (XSD_IPADDRESS.equals(type)) {
            value = new IpAddressType(marshalledValue).getDelegate();
        }
    }
    
    public static Schema<TypedValue> getSchema() {
        return SCHEMA;
    }
    
    @Override
    public Schema<TypedValue> cachedSchema() {
        return SCHEMA;
    }
    
    @XmlTransient
    private static final Schema<TypedValue> SCHEMA = new Schema<TypedValue>() {
        
        @Override
        public TypedValue newMessage() {
            return new TypedValue();
        }
        
        @Override
        public Class<? super TypedValue> typeClass() {
            return TypedValue.class;
        }
        
        @Override
        public String messageName() {
            return TypedValue.class.getSimpleName();
        }
        
        @Override
        public String messageFullName() {
            return TypedValue.class.getName();
        }
        
        @Override
        public boolean isInitialized(TypedValue message) {
            return true;
        }
        
        @Override
        public void writeTo(Output output, TypedValue message) throws IOException {
            Class<?> clazz = message.value.getClass();
            if (Type.class.isAssignableFrom(clazz)) {
                message.value = ((Type<?>) message.value).getDelegate();
                clazz = message.value.getClass();
            }
            if (String.class.equals(clazz)) {
                output.writeString(1, (String) message.value, false);
            } else if (Byte.class.equals(clazz)) {
                output.writeInt32(2, (Byte) message.value, false);
            } else if (byte[].class.equals(clazz)) {
                output.writeByteArray(3, (byte[]) message.value, false);
            } else if (Date.class.equals(clazz)) {
                output.writeInt64(4, ((Date) message.value).getTime(), false);
            } else if (Calendar.class.equals(clazz)) {
                output.writeInt64(4, ((Calendar) message.value).getTimeInMillis(), false);
            } else if (BigDecimal.class.equals(clazz)) {
                output.writeString(5, ((BigDecimal) message.value).toString(), false);
            } else if (Short.class.equals(clazz)) {
                output.writeInt32(6, (Short) message.value, false);
            } else if (Integer.class.equals(clazz)) {
                output.writeInt32(7, (Integer) message.value, false);
            } else if (BigInteger.class.equals(clazz)) {
                output.writeString(8, ((BigInteger) message.value).toString(), false);
            } else if (Long.class.equals(clazz)) {
                output.writeInt64(9, (Long) message.value, false);
            } else if (Float.class.equals(clazz)) {
                output.writeFloat(10, (Float) message.value, false);
            } else if (Double.class.equals(clazz)) {
                output.writeDouble(11, (Double) message.value, false);
            } else if (Boolean.class.equals(clazz)) {
                output.writeBool(12, (Boolean) message.value, false);
            }
        }
        
        @Override
        public void mergeFrom(Input input, TypedValue message) throws IOException {
            int number;
            while ((number = input.readFieldNumber(this)) != 0) {
                switch (number) {
                    case 1:
                        message.setValue(input.readString());
                        break;
                    case 2:
                        message.setValue((byte) input.readInt32());
                        break;
                    case 3:
                        message.setValue(input.readByteArray());
                        break;
                    case 4:
                        Calendar cal = Calendar.getInstance();
                        cal.setTimeInMillis(input.readInt64());
                        message.setValue(cal);
                        break;
                    case 5:
                        message.setValue(new BigDecimal(input.readString()));
                        break;
                    case 6:
                        message.setValue((short) input.readInt32());
                        break;
                    case 7:
                        message.setValue(input.readInt32());
                        break;
                    case 8:
                        message.setValue(new BigInteger(input.readString()));
                        break;
                    case 9:
                        message.setValue(input.readInt64());
                        break;
                    case 10:
                        message.setValue(input.readFloat());
                        break;
                    case 11:
                        message.setValue(input.readDouble());
                        break;
                    case 12:
                        message.setValue(input.readBool());
                        break;
                    default:
                        input.handleUnknownField(number, this);
                        break;
                }
            }
        }
        
        @Override
        public String getFieldName(int number) {
            switch (number) {
                case 1:
                    return "stringValue";
                case 2:
                    return "byteValue";
                case 3:
                    return "byteArrayValue";
                case 4:
                    return "dateTimeValue";
                case 5:
                    return "decimalValue";
                case 6:
                    return "shortValue";
                case 7:
                    return "intValue";
                case 8:
                    return "integerValue";
                case 9:
                    return "longValue";
                case 10:
                    return "floatValue";
                case 11:
                    return "doubleValue";
                case 12:
                    return "booleanValue";
                default:
                    return null;
            }
        }
        
        @Override
        public int getFieldNumber(String name) {
            final Integer number = fieldMap.get(name);
            return number == null ? 0 : number.intValue();
        }
        
        private final HashMap<String,Integer> fieldMap = new HashMap<String,Integer>();
        {
            fieldMap.put("stringValue", 1);
            fieldMap.put("byteValue", 2);
            fieldMap.put("byteArrayValue", 3);
            fieldMap.put("dateTimeValue", 4);
            fieldMap.put("decimalValue", 5);
            fieldMap.put("shortValue", 6);
            fieldMap.put("intValue", 7);
            fieldMap.put("integerValue", 8);
            fieldMap.put("longValue", 9);
            fieldMap.put("floatValue", 10);
            fieldMap.put("doubleValue", 11);
            fieldMap.put("booleanValue", 12);
        }
    };
}
