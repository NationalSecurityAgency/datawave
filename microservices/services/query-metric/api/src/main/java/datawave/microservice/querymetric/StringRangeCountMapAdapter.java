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
 * Provides JAX-B marshalling/unmarshalling of {@link Map} of String to RangeCount. This allows the marshalled type to be in our own namespace rather than in
 * the "default" one, which when triggered will then assume the "" prefix and push all of our own elements into the "ns2" prefix.
 */
public class StringRangeCountMapAdapter extends XmlAdapter<StringRangeCountMapAdapter.StringRangeCountMap,Map<String,RangeCounts>> {
    
    @Override
    public Map<String,RangeCounts> unmarshal(StringRangeCountMap v) throws Exception {
        HashMap<String,RangeCounts> map = new HashMap<>();
        for (StringRangeCountMapEntry entry : v.entries) {
            RangeCounts unmarshalledRangeCounts = new RangeCounts();
            unmarshalledRangeCounts.setDocumentRangeCount(Long.parseLong(entry.value.split(",")[0]));
            unmarshalledRangeCounts.setShardRangeCount(Long.parseLong(entry.value.split(",")[1]));
            map.put(entry.key, unmarshalledRangeCounts);
        }
        return map;
    }
    
    @Override
    public StringRangeCountMap marshal(Map<String,RangeCounts> v) throws Exception {
        StringRangeCountMap map = new StringRangeCountMap();
        for (Map.Entry<String,RangeCounts> entry : v.entrySet()) {
            map.entries.add(new StringRangeCountMapEntry(entry.getKey(), toText(entry.getValue())));
        }
        return map;
    }
    
    public String toText(RangeCounts counts) {
        return counts.getDocumentRangeCount() + "," + counts.getShardRangeCount();
    }
    
    public static class StringRangeCountMap {
        @XmlElement(name = "entry")
        private List<StringRangeCountMapEntry> entries = new ArrayList<>();
    }
    
    public static class StringRangeCountMapEntry {
        @XmlAttribute(name = "name")
        private String key;
        @XmlValue
        private String value;
        
        public StringRangeCountMapEntry() {}
        
        public StringRangeCountMapEntry(String key, String value) {
            this.key = key;
            this.value = value;
        }
    }
}
