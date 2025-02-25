package datawave.webservice.xml.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.adapters.XmlAdapter;

/**
 * Provides JAX-B marshalling/unmarshalling of {@link Map} of String to String. This allows the marshalled type to be in our own namespace rather than in the
 * "default" one, which when triggered will then assume the "" prefix and push all of our own elements into the "ns2" prefix.
 */
public class StringMapAdapter extends XmlAdapter<StringMapAdapter.StringMap,Map<String,String>> {
    
    @Override
    public Map<String,String> unmarshal(StringMap v) throws Exception {
        HashMap<String,String> map = new HashMap<String,String>();
        if (v != null) {
            for (StringMapEntry entry : v.entries) {
                map.put(entry.key, entry.value);
            }
        }
        return map;
    }
    
    @Override
    public StringMap marshal(Map<String,String> v) throws Exception {
        StringMap map = new StringMap();
        if (v != null) {
            for (Map.Entry<String,String> entry : v.entrySet()) {
                map.entries.add(new StringMapEntry(entry.getKey(), entry.getValue()));
            }
        }
        return map;
    }
    
    public static class StringMap {
        @XmlElement(name = "entry")
        private List<StringMapEntry> entries = new ArrayList<StringMapEntry>();
    }
    
    public static class StringMapEntry {
        @XmlElement(name = "key")
        private String key;
        @XmlElement(name = "value")
        private String value;
        
        public StringMapEntry() {}
        
        public StringMapEntry(String key, String value) {
            this.key = key;
            this.value = value;
        }
    }
}
