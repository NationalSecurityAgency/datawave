package datawave.query.discovery;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Sets;
import datawave.query.Constants;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.javatuples.Pair;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Set;

import static datawave.query.discovery.IndexMatchingIterator.CONF;
import static datawave.query.discovery.IndexMatchingIterator.REVERSE_INDEX;
import static datawave.query.discovery.IndexMatchingIterator.gson;
import static org.junit.Assert.assertEquals;

public class IndexMatchingIteratorTest {
    static Set<Pair<String,String>> terms;
    static ImmutableSortedMap<Key,Value> data;
    static ImmutableSortedMap<Key,Value> reverseData;
    static ImmutableSortedMap<Key,Value> reverseData2;
    static Value blank;
    
    @BeforeClass
    public static void setUp() {
        blank = new Value(new byte[0]);
        terms = Sets.newHashSet(Pair.with("firetruck", "vehicle"), Pair.with("ruddy duck", "bird"), Pair.with("ruddy duck", "unidentified flying object"),
                        Pair.with("motorcycle", "vehicle"), Pair.with("motorboat", "vehicle"), Pair.with("strike", "actionable offense"),
                        Pair.with("car", "vehicle"), Pair.with("trophy", "prize"), Pair.with("police officer", "otherperson"),
                        Pair.with("skydiver", "occupation"), Pair.with("bbc", "network"), Pair.with("onyx", "pokemon"), Pair.with("onyx", "rock"),
                        Pair.with("onyx", "rooster"), Pair.with("rooster", "cockadoodledoo"));
        ImmutableSortedMap.Builder<Key,Value> builder = ImmutableSortedMap.naturalOrder();
        for (Pair<String,String> term : terms) {
            builder.put(new Key(term.getValue0(), term.getValue1()), blank);
        }
        data = builder.build();
        
        builder = ImmutableSortedMap.naturalOrder();
        for (Pair<String,String> term : terms) {
            builder.put(new Key(new StringBuilder().append(term.getValue0()).reverse().toString(), term.getValue1()), blank);
        }
        reverseData = builder.build();
        
        terms = Sets.newHashSet(Pair.with("skydiver", "job"), Pair.with("skydiver", "job"), Pair.with("skydiver", "job"), Pair.with("skydiver", "job"),
                        Pair.with("skydiver", "occupation"), Pair.with("skydiver", "occupation"), Pair.with("skydiver", "occupation"),
                        Pair.with("skydiver", "occupation"), Pair.with("skydiver", "occupation"), Pair.with("skydiver", "occupation"),
                        Pair.with("skydiver", "occupation"), Pair.with("xxx.skydiver", "occupation"), Pair.with("xxx.skydiver", "occupation"),
                        Pair.with("xxx.skydiver", "occupation"), Pair.with("xxx.skydiver", "occupation"), Pair.with("xxx.skydiver", "occupation"),
                        Pair.with("yyy.skydiver", "occupation"), Pair.with("yyy.skydiver", "occupation"), Pair.with("yyy.skydiver", "occupation"),
                        Pair.with("zskydiver", "occupation"));
        
        builder = ImmutableSortedMap.naturalOrder();
        for (Pair<String,String> term : terms) {
            builder.put(new Key(new StringBuilder().append(term.getValue0()).reverse().toString(), term.getValue1()), blank);
        }
        reverseData2 = builder.build();
        
    }
    
    @Test
    public void testUnfieldedLiterals() throws Throwable {
        IndexMatchingIterator.Configuration conf = new IndexMatchingIterator.Configuration();
        conf.addLiteral("bbc");
        conf.addLiteral("onyx");
        
        Set<String> matches = Sets.newHashSet();
        IndexMatchingIterator itr = new IndexMatchingIterator();
        itr.init(new SortedMapIterator(data), ImmutableMap.of(CONF, gson().toJson(conf)), null);
        itr.seek(new Range(), new ArrayList<>(), false);
        while (itr.hasTop()) {
            matches.add(itr.getTopKey().getRow().toString());
            itr.next();
        }
        assertEquals(ImmutableSet.of("bbc", "onyx"), matches);
    }
    
    @Test
    public void testUnfieldedPatterns() throws Throwable {
        IndexMatchingIterator.Configuration conf = new IndexMatchingIterator.Configuration();
        conf.addPattern(".*er");
        conf.addPattern(".+o.+");
        
        Set<String> matches = Sets.newHashSet();
        IndexMatchingIterator itr = new IndexMatchingIterator();
        itr.init(new SortedMapIterator(data), ImmutableMap.of(CONF, gson().toJson(conf)), null);
        itr.seek(new Range(), new ArrayList<>(), false);
        while (itr.hasTop()) {
            matches.add(itr.getTopKey().getRow().toString());
            itr.next();
        }
        assertEquals(ImmutableSet.of("skydiver", "police officer", "motorcycle", "motorboat", "rooster", "trophy"), matches);
    }
    
    @Test
    public void testUnfielded() throws Throwable {
        IndexMatchingIterator.Configuration conf = new IndexMatchingIterator.Configuration();
        conf.addPattern(".*er");
        conf.addLiteral("trophy");
        
        Set<String> matches = Sets.newHashSet();
        IndexMatchingIterator itr = new IndexMatchingIterator();
        itr.init(new SortedMapIterator(data), ImmutableMap.of(CONF, gson().toJson(conf)), null);
        itr.seek(new Range(), new ArrayList<>(), false);
        while (itr.hasTop()) {
            matches.add(itr.getTopKey().getRow().toString());
            itr.next();
        }
        assertEquals(ImmutableSet.of("skydiver", "police officer", "rooster", "trophy"), matches);
    }
    
    @Test
    public void testFieldedLiteral() throws Throwable {
        IndexMatchingIterator.Configuration conf = new IndexMatchingIterator.Configuration();
        conf.addLiteral("onyx", "pokemon");
        conf.addLiteral("onyx", "rock");
        
        Set<Pair<String,String>> matches = Sets.newHashSet();
        IndexMatchingIterator itr = new IndexMatchingIterator();
        itr.init(new SortedMapIterator(data), ImmutableMap.of(CONF, gson().toJson(conf)), null);
        itr.seek(new Range(), new ArrayList<>(), false);
        while (itr.hasTop()) {
            matches.add(parse(itr.getTopKey()));
            itr.next();
        }
        assertEquals(ImmutableSet.of(Pair.with("onyx", "pokemon"), Pair.with("onyx", "rock")), matches);
    }
    
    @Test
    public void testFieldedPattern() throws Throwable {
        IndexMatchingIterator.Configuration conf = new IndexMatchingIterator.Configuration();
        conf.addPattern(".*r.*k", "vehicle");
        conf.addPattern(".*r.*k", "bird");
        
        Set<Pair<String,String>> matches = Sets.newHashSet();
        IndexMatchingIterator itr = new IndexMatchingIterator();
        itr.init(new SortedMapIterator(data), ImmutableMap.of(CONF, gson().toJson(conf)), null);
        itr.seek(new Range(), new ArrayList<>(), false);
        while (itr.hasTop()) {
            matches.add(parse(itr.getTopKey()));
            itr.next();
        }
        assertEquals(ImmutableSet.of(Pair.with("firetruck", "vehicle"), Pair.with("ruddy duck", "bird")), matches);
    }
    
    @Test
    public void testFielded() throws Throwable {
        IndexMatchingIterator.Configuration conf = new IndexMatchingIterator.Configuration();
        conf.addPattern("onyx", "pokemon");
        conf.addPattern(".*r.*k", "bird");
        
        Set<Pair<String,String>> matches = Sets.newHashSet();
        IndexMatchingIterator itr = new IndexMatchingIterator();
        itr.init(new SortedMapIterator(data), ImmutableMap.of(CONF, gson().toJson(conf)), null);
        itr.seek(new Range(), new ArrayList<>(), false);
        while (itr.hasTop()) {
            matches.add(parse(itr.getTopKey()));
            itr.next();
        }
        assertEquals(ImmutableSet.of(Pair.with("onyx", "pokemon"), Pair.with("ruddy duck", "bird")), matches);
    }
    
    @Test
    public void testMix() throws Throwable {
        IndexMatchingIterator.Configuration conf = new IndexMatchingIterator.Configuration();
        conf.addLiteral("skydiver");
        conf.addPattern("onyx", "rock");
        conf.addPattern(".*r.*k");
        conf.addPattern("motor.*", "vehicle");
        
        Set<Pair<String,String>> matches = Sets.newHashSet();
        IndexMatchingIterator itr = new IndexMatchingIterator();
        itr.init(new SortedMapIterator(data), ImmutableMap.of(CONF, gson().toJson(conf)), null);
        itr.seek(new Range(), new ArrayList<>(), false);
        while (itr.hasTop()) {
            matches.add(parse(itr.getTopKey()));
            itr.next();
        }
        assertEquals(ImmutableSet.of(Pair.with("onyx", "rock"), Pair.with("ruddy duck", "bird"), Pair.with("firetruck", "vehicle"),
                        Pair.with("motorboat", "vehicle"), Pair.with("motorcycle", "vehicle"), Pair.with("skydiver", "occupation"),
                        Pair.with("ruddy duck", "unidentified flying object")), matches);
        
    }
    
    @Test
    public void testReverse() throws Throwable {
        Logger.getLogger(IndexMatchingIterator.class).setLevel(Level.DEBUG);
        IndexMatchingIterator.Configuration conf = new IndexMatchingIterator.Configuration();
        conf.addPattern(".*\\.sky.*er");
        
        Set<Pair<String,String>> matches = Sets.newHashSet();
        IndexMatchingIterator itr = new IndexMatchingIterator();
        itr.init(new SortedMapIterator(reverseData2), ImmutableMap.of(CONF, gson().toJson(conf), REVERSE_INDEX, "true"), null);
        Range r = new Range(new Key("re"), true, new Key("re" + Constants.MAX_UNICODE_STRING), false);
        itr.seek(r, new ArrayList<>(), false);
        Key topKey = null;
        int count = 0;
        while (itr.hasTop()) {
            topKey = itr.getTopKey();
            matches.add(Pair.with(new StringBuilder().append(topKey.getRow()).reverse().toString(), topKey.getColumnFamily().toString()));
            if (count++ == 15) {
                r = new Range(topKey, false, new Key("re" + Constants.MAX_UNICODE_STRING), false);
                itr.seek(r, new ArrayList<>(), false);
            } else {
                itr.next();
            }
        }
        
        assertEquals(ImmutableSet.of(Pair.with("xxx.skydiver", "occupation"), Pair.with("yyy.skydiver", "occupation")), matches);
    }
    
    public static Pair<String,String> parse(Key k) {
        return Pair.with(k.getRow().toString(), k.getColumnFamily().toString());
    }
}
