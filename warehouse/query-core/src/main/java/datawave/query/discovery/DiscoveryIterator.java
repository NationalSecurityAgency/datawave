package datawave.query.discovery;

import com.google.common.base.Predicates;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Collections2.filter;
import static com.google.common.collect.Collections2.transform;
import static com.google.common.collect.Lists.newArrayList;

public class DiscoveryIterator implements SortedKeyValueIterator<Key,Value> {
    private static final Logger log = Logger.getLogger(DiscoveryIterator.class);
    
    private Key tk;
    private Value tv;
    private SortedKeyValueIterator<Key,Value> itr;
    private boolean separateCountsByColVis = false;
    private boolean showReferenceCount = false;
    private boolean reverseIndex = false;
    
    @Override
    public DiscoveryIterator deepCopy(IteratorEnvironment env) {
        DiscoveryIterator i = new DiscoveryIterator();
        i.itr = itr.deepCopy(env);
        return i;
    }
    
    @Override
    public void next() throws IOException {
        tk = null;
        tv = null;
        
        while (itr.hasTop() && tk == null) {
            Multimap<String,TermInfo> terms = aggregateDate();
            if (terms.isEmpty()) {
                if (log.isTraceEnabled())
                    log.trace("Couldn't aggregate index info; moving onto next date/field/term if data is available.");
                continue;
            } else {
                if (log.isTraceEnabled())
                    log.trace("Received term info multimap of size [" + terms.size() + "]");
                ArrayList<DiscoveredThing> things = newArrayList(filter(
                                transform(terms.asMap().values(), new TermInfoAggregation(separateCountsByColVis, showReferenceCount, reverseIndex)),
                                Predicates.notNull()));
                if (log.isTraceEnabled())
                    log.trace("After conversion to discovery objects, there are [" + things.size() + "] term info objects.");
                if (things.isEmpty()) {
                    continue;
                } else {
                    Pair<Key,Value> top = makeTop(things);
                    tk = top.getFirst();
                    tv = top.getSecond();
                    return;
                }
            }
        }
        if (log.isTraceEnabled())
            log.trace("No data found.");
    }
    
    private Multimap<String,TermInfo> aggregateDate() throws IOException {
        Multimap<String,TermInfo> terms = ArrayListMultimap.create();
        Key start = new Key(itr.getTopKey()), key = null;
        while (itr.hasTop() && start.equals((key = itr.getTopKey()), PartialKey.ROW_COLFAM) && datesMatch(start, key)) {
            TermInfo ti = new TermInfo(key, itr.getTopValue());
            if (ti.valid)
                terms.put(ti.datatype, ti);
            else {
                if (log.isTraceEnabled())
                    log.trace("Received invalid term info from key: " + key);
            }
            itr.next();
        }
        return terms;
    }
    
    private static boolean datesMatch(Key reference, Key test) {
        ByteSequence a = reference.getColumnQualifierData(), b = test.getColumnQualifierData();
        for (int i = 0; i < 8; i++) {
            if (a.byteAt(i) != b.byteAt(i)) {
                return false;
            }
        }
        return true;
    }
    
    private Pair<Key,Value> makeTop(List<DiscoveredThing> things) {
        Writable[] returnedThings = new Writable[things.size()];
        for (int i = 0; i < returnedThings.length; ++i)
            returnedThings[i] = things.get(i);
        ArrayWritable aw = new ArrayWritable(DiscoveredThing.class);
        aw.set(returnedThings);
        
        DiscoveredThing thing = things.get(0);
        // we want the key to be the last possible key for this date
        return new Pair<>(new Key(thing.getTerm(), thing.getField(), thing.getDate() + '\uffff'), new Value(WritableUtils.toByteArray(aw)));
    }
    
    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        
        itr.seek(range, columnFamilies, inclusive);
        if (log.isTraceEnabled())
            log.trace("My source " + (itr.hasTop() ? "does" : "does not") + " have a top.");
        next();
    }
    
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        itr = source;
        separateCountsByColVis = Boolean.parseBoolean(options.get(DiscoveryLogic.SEPARATE_COUNTS_BY_COLVIS));
        showReferenceCount = Boolean.parseBoolean(options.get(DiscoveryLogic.SHOW_REFERENCE_COUNT));
        reverseIndex = Boolean.parseBoolean(options.get(DiscoveryLogic.REVERSE_INDEX));
        
        if (log.isTraceEnabled()) {
            log.trace("My source is a " + source.getClass().getName());
            log.trace("Separate counts by column visibility = " + separateCountsByColVis);
            log.trace("Show reference count only = " + showReferenceCount);
        }
    }
    
    @Override
    public boolean hasTop() {
        return tk != null;
    }
    
    @Override
    public Key getTopKey() {
        return tk;
    }
    
    @Override
    public Value getTopValue() {
        return tv;
    }
}
