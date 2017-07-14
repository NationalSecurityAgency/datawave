package datawave.webservice.query.util;

import javax.xml.bind.annotation.adapters.XmlAdapter;

public class TypedValueAdapter extends XmlAdapter<TypedValue,Object> {
    
    @Override
    public Object unmarshal(TypedValue v) throws Exception {
        return v.getValue();
    }
    
    @Override
    public TypedValue marshal(Object v) throws Exception {
        return new TypedValue(v);
    }
}
