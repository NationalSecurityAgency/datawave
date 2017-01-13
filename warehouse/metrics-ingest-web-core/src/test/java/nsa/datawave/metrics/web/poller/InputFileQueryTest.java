package nsa.datawave.metrics.web.poller;

import java.util.Collections;
import java.util.TreeMap;

import nsa.datawave.metrics.web.poller.InputFileQuery.InputFileFilter;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.log4j.Logger;
import org.junit.Test;

import com.google.common.collect.Maps;

public class InputFileQueryTest {
    
    private static final Logger log = Logger.getLogger(InputFileQueryTest.class);
    
    @Test
    public void testIterator() throws Throwable {
        TreeMap<Key,Value> data = Maps.newTreeMap();
        final Value blank = new Value(new byte[0]);
        data.put(new Key("type.1_20130101000000_host_suffix"), blank);
        data.put(new Key("type.2_20130101000001_host_suffix"), blank);
        data.put(new Key("type.3_20130101000000_host_suffix"), blank);
        data.put(new Key("type.4_20130101000000_host_suffix"), blank);
        data.put(new Key("uvula_0130101000000_host_suffix"), blank);
        data.put(new Key("type_20130101000000_host_suffix"), blank);
        SortedMapIterator src = new SortedMapIterator(data);
        InputFileFilter filter = new InputFileFilter();
        TreeMap<String,String> opts = Maps.newTreeMap();
        opts.put(InputFileFilter.END, "20130101000000");
        opts.put(InputFileFilter.START, "20130101000000");
        opts.put(InputFileFilter.TYPE, "type");
        filter.init(src, opts, null);
        filter.seek(new Range("type.", "type\uffff"), Collections.<ByteSequence> emptySet(), false);
        while (filter.hasTop()) {
            log.debug(filter.getTopKey());
            filter.next();
        }
    }
}
