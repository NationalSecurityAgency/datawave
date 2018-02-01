package datawave.webservice.query.util;

import javax.xml.bind.annotation.adapters.XmlAdapter;

/**
 * An {@link XmlAdapter} that allows a {@link String} property to be bound to XML that is encoded as an {@link OptionallyEncodedStringAdapter}.
 * 
 * @see OptionallyEncodedStringAdapter
 */
public class OptionallyEncodedStringAdapter extends XmlAdapter<OptionallyEncodedString,String> {
    
    @Override
    public String unmarshal(OptionallyEncodedString v) throws Exception {
        return v.getValue();
    }
    
    @Override
    public OptionallyEncodedString marshal(String v) throws Exception {
        return (v == null) ? null : new OptionallyEncodedString(v);
    }
    
}
