package datawave.query.iterator;

import static datawave.query.iterator.QueryOptions.COLLECT_TIMING_DETAILS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.junit.Before;
import org.junit.Test;

/**
 * Integration test for WaitWindowObserver using the QueryIteratorIT and the SerialIterator
 */
public class WaitWindowQueryIteratorSerialIT extends QueryIteratorIT {

    @Override
    protected Class getIteratorClass() {
        return WaitWindowQueryIterator.class;
    }

    @Before
    public void setCollectDetails() {
        options.put(COLLECT_TIMING_DETAILS, "true");
    }

    private void createEvents(int numEvents, List<Map.Entry<Key,Map<String,List<String>>>> otherHits, List<Map.Entry<Key,Value>> otherData) {
        for (int x = 0; x < numEvents; x++) {
            List<Map.Entry<Key,Value>> l = new ArrayList<>();
            String uid = "123.345." + String.format("%04d", x);
            l.addAll(addEvent(uid));
            int mod = x % 3;
            if (mod == 1 || mod == 2) {
                l = l.stream().filter(e -> !e.getKey().getColumnQualifier().toString().startsWith("EVENT_FIELD1")).collect(Collectors.toList());
                l = l.stream().filter(e -> !e.getKey().getColumnFamily().toString().endsWith("EVENT_FIELD1")).collect(Collectors.toList());
            }
            if (mod == 0 || mod == 2) {
                l = l.stream().filter(e -> !e.getKey().getColumnQualifier().toString().startsWith("EVENT_FIELD4")).collect(Collectors.toList());
                l = l.stream().filter(e -> !e.getKey().getColumnFamily().toString().endsWith("EVENT_FIELD4")).collect(Collectors.toList());
            }
            if (mod == 0 || mod == 1) {
                l = l.stream().filter(e -> !e.getKey().getColumnQualifier().toString().startsWith("EVENT_FIELD6")).collect(Collectors.toList());
                l = l.stream().filter(e -> !e.getKey().getColumnFamily().toString().endsWith("EVENT_FIELD6")).collect(Collectors.toList());
            }
            if (mod == 3) {
                otherHits.add(getBaseExpectedEvent(uid));
            }
            otherData.addAll(l);
        }
    }

    @Test
    public void many_events() throws IOException {
        List<Map.Entry<Key,Map<String,List<String>>>> otherHits = new ArrayList<>();
        List<Map.Entry<Key,Value>> otherData = new ArrayList<>();
        createEvents(2000, otherHits, otherData);
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD1 == 'a' && EVENT_FIELD4 == 'd' && EVENT_FIELD6 == 'f'";
        tf_test(seekRange, query, getBaseExpectedEvent("123.345.456"), otherData, otherHits);
    }
}
