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
 * Provides JAX-B marshalling/unmarshalling of {@link Map} of String to Long. This allows the marshalled type to be in our own namespace rather than in the
 * "default" one, which when triggered will then assume the "" prefix and push all of our own elements into the "ns2" prefix.
 */
public class LongMapAdapter extends XmlAdapter<LongMapAdapter.LongMap,Map<String,Long>> {
    
    @Override
    public Map<String,Long> unmarshal(LongMapAdapter.LongMap v) throws Exception {
        HashMap<String,Long> map = new HashMap<>();
        for (LongMapAdapter.LongMapEntry entry : v.entries) {
            map.put(entry.key, entry.value);
        }
        return map;
    }
    
    @Override
    public LongMap marshal(Map<String,Long> v) throws Exception {
        LongMapAdapter.LongMap map = new LongMapAdapter.LongMap();
        for (Map.Entry<String,Long> entry : v.entrySet()) {
            map.entries.add(new LongMapAdapter.LongMapEntry(entry.getKey(), entry.getValue()));
        }
        return map;
    }
    
    public static class LongMap {
        @XmlElement(name = "entry")
        private List<LongMapAdapter.LongMapEntry> entries = new ArrayList<>();
    }
    
    public static class LongMapEntry {
        @XmlAttribute(name = "name")
        private String key;
        @XmlValue
        private Long value;
        
        public LongMapEntry() {}
        
        public LongMapEntry(String key, Long value) {
            this.key = key;
            this.value = value;
        }
    }
}
