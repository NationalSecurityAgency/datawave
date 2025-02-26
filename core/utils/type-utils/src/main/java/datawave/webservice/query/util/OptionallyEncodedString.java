package datawave.webservice.query.util;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlValue;

import org.apache.commons.codec.binary.Base64;

/**
 * A JAXB holder class for strings that could possibly contain invalid XML characters. If any invalid XML characters are found, the string will be base64
 * encoded.
 * <p>
 * <strong>Note:</strong> Consider not using this class directly, but rather keeping your underlying type as a string, and using the
 * {@link OptionallyEncodedStringAdapter} instead. Here is an example:
 * 
 * <pre>
 *     &#64;XmlRootElement
 *     class DataClass {
 *         &#64;XmlElement(name="DataString")
 *         &#64;XmlJavaTypeAdapter(OptionallyEncodedStringAdapter.class)
 *         private String dataString;
 *         
 *         ...
 *     };
 * </pre>
 * 
 * You can work with the {@code dataString} field normally, but it will marshal/unmarshall to the OptionallyEncodedString format. That is, if {@code dataString}
 * contained an invalid XML character, then the marshalled XML would look like:
 * 
 * <pre>
 * {@code
 * 
 *     <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
 *     <DataClass>
 *         <DataString base64Encoded="true">BASE_64_STUFF_HERE</DataString>
 *     </DataClass>
 * }
 * </pre>
 * 
 * @see OptionallyEncodedStringAdapter
 */
@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class OptionallyEncodedString {
    
    @XmlAttribute
    private Boolean base64Encoded = null;
    
    @XmlValue
    private String value = null;
    
    public OptionallyEncodedString() {}
    
    public OptionallyEncodedString(String value) {
        setValue(value);
    }
    
    public Boolean getBase64Encoded() {
        return base64Encoded;
    }
    
    public String getValue() {
        if (this.base64Encoded != null && this.base64Encoded.equals(Boolean.TRUE)) {
            byte[] incoming;
            String decoded = null;
            
            try {
                incoming = value.getBytes("UTF-8");
                byte[] decodedBytes = Base64.decodeBase64(incoming);
                decoded = new String(decodedBytes, Charset.forName("UTF-8"));
            } catch (UnsupportedEncodingException e) {
                // Should never happen with UTF-8!!! (but if it does we will be
                // returning a null)
            }
            
            return decoded;
        } else {
            return value;
        }
    }
    
    public byte[] getValueAsBytes() {
        try {
            byte[] incoming = value.getBytes("UTF-8");
            if (this.base64Encoded != null && this.base64Encoded.equals(Boolean.TRUE)) {
                return Base64.decodeBase64(incoming);
            } else {
                return incoming;
            }
        } catch (UnsupportedEncodingException e) {
            // Should never happen with UTF-8!!! (but if it does we will be
            // returning a null)
        }
        
        // Should never get here
        return null;
    }
    
    public void setBase64Encoded(Boolean base64Encoded) {
        this.base64Encoded = base64Encoded;
    }
    
    public void setValue(String value) {
        if (XMLUtil.isValidXML(value)) {
            this.value = value;
        } else {
            this.value = new String(Base64.encodeBase64(value.getBytes(UTF_8)), UTF_8);
            this.base64Encoded = true;
        }
    }
}
