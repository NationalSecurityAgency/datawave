package datawave.microservice.querymetric;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlValue;
import javax.xml.bind.annotation.adapters.XmlAdapter;

/**
 * Provides JAX-B marshalling/unmarshalling of {@link Map} of String to String. This allows the marshalled type to be in our own namespace rather than in the
 * "default" one, which when triggered will then assume the "" prefix and push all of our own elements into the "ns2" prefix.
 */
public class StringMapAdapter extends XmlAdapter<StringMapAdapter.StringMap,Map<String,String>> {
    
    @Override
    public Map<String,String> unmarshal(StringMapAdapter.StringMap v) throws Exception {
        HashMap<String,String> map = new HashMap<>();
        for (StringMapEntry entry : v.entries) {
            map.put(entry.key, entry.value);
        }
        return map;
    }
    
    @Override
    public StringMap marshal(Map<String,String> v) throws Exception {
        StringMapAdapter.StringMap map = new StringMapAdapter.StringMap();
        for (Map.Entry<String,String> entry : v.entrySet()) {
            map.entries.add(new StringMapAdapter.StringMapEntry(entry.getKey(), entry.getValue()));
        }
        return map;
    }
    
    public static class StringMap {
        @XmlElement(name = "entry")
        private List<StringMapAdapter.StringMapEntry> entries = new ArrayList<>();
    }
    
    public static class StringMapEntry {
        @XmlAttribute(name = "name")
        private String key;
        @XmlValue
        private String value;
        
        public StringMapEntry() {}
        
        public StringMapEntry(String key, String value) {
            this.key = key;
            this.value = value;
        }
    }
}
