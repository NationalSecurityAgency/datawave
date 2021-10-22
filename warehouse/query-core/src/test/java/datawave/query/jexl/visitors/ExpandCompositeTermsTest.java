package datawave.query.jexl.visitors;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import datawave.data.normalizer.NoOpNormalizer;
import datawave.data.normalizer.Normalizer;
import datawave.data.type.BaseType;
import datawave.data.type.DiscreteIndexType;
import datawave.data.type.GeometryType;
import datawave.data.type.Type;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.util.DateIndexHelper;
import datawave.query.util.MockMetadataHelper;
import datawave.test.JexlNodeAssert;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class ExpandCompositeTermsTest {
    
    private static final Set<String> INDEX_FIELDS = Sets.newHashSet("MAKE", "COLOR", "WHEELS", "TEAM", "NAME", "POINTS");
    
    private static Multimap<String,String> compositeToFieldMap;
    
    private static Map<String,String> compositeFieldSeparators;
    
    private ShardQueryConfiguration conf;
    
    @BeforeClass
    public static void beforeClass() {
        Multimap<String,String> multimap = LinkedListMultimap.create();
        multimap.put("MAKE_COLOR", "MAKE");
        multimap.put("MAKE_COLOR", "COLOR");
        multimap.put("COLOR_WHEELS", "COLOR");
        multimap.put("COLOR_WHEELS", "WHEELS");
        multimap.put("TEAM_NAME_POINTS", "TEAM");
        multimap.put("TEAM_NAME_POINTS", "NAME");
        multimap.put("TEAM_NAME_POINTS", "POINTS");
        multimap.put("TEAM_POINTS", "TEAM");
        multimap.put("TEAM_POINTS", "POINTS");
        compositeToFieldMap = Multimaps.unmodifiableMultimap(multimap);
        
        Map<String,String> sepMap = new HashMap<>();
        sepMap.put("MAKE_COLOR", ",");
        sepMap.put("COLOR_WHEELS", ",");
        sepMap.put("TEAM_NAME_POINTS", ",");
        sepMap.put("TEAM_POINTS", ",");
        compositeFieldSeparators = Collections.unmodifiableMap(sepMap);
    }
    
    @Before
    public void before() {
        conf = new ShardQueryConfiguration();
        conf.setCompositeToFieldMap(compositeToFieldMap);
        conf.setCompositeFieldSeparators(compositeFieldSeparators);
    }
    
    @Test
    public void test() throws Exception {
        workIt("MAKE == 'Ford' && COLOR == 'red'");
        workIt("MAKE == 'Ford' && COLOR == 'red' && MAKE_COLOR == 'Fordred'");
        workIt("(MAKE == 'Ford' && WHEELS == 3) && COLOR == 'red'");
    }
    
    @Test
    public void test2() throws Exception {
        String query = "WINNER=='blue' && TEAM=='gold' && POINTS==11";
        String expected = "WINNER == 'blue' && TEAM_POINTS == 'gold,11'";
        
        runTestQuery(query, expected);
    }
    
    @Test
    public void test3() throws Exception {
        String query = "WINNER=='blue' && TEAM=='gold' && NAME=='gold-8' && POINTS==11";
        runTestQuery(query, "WINNER == 'blue' && TEAM_NAME_POINTS == 'gold,gold-8,11'");
    }
    
    @Test
    public void test4a() throws Exception {
        String query = "WINNER=='blue' && TEAM=='gold' && NAME=='gold-8' && ((_Bounded_ = true) && (POINTS > 10 && POINTS <= 11))";
        String expected = "WINNER == 'blue' && ((_Bounded_ = true) && (TEAM_NAME_POINTS > 'gold,gold-8,10' && TEAM_NAME_POINTS <= 'gold,gold-8,11')) && ((_Eval_ = true) && (TEAM == 'gold' && NAME == 'gold-8' && ((_Bounded_ = true) && (POINTS > 10 && POINTS <= 11))))";
        runTestQuery(query, expected);
    }
    
    @Test
    public void test4b() throws Exception {
        String query = "WINNER=='blue' && (TEAM=='gold') && (NAME=='gold-8') && ((_Bounded_ = true) && (POINTS > 10 && POINTS <= 11))";
        String expected = "WINNER == 'blue' && ((_Bounded_ = true) && (TEAM_NAME_POINTS > 'gold,gold-8,10' && TEAM_NAME_POINTS <= 'gold,gold-8,11')) && ((_Eval_ = true) && (TEAM == 'gold' && NAME == 'gold-8' && ((_Bounded_ = true) && (POINTS > 10 && POINTS <= 11))))";
        runTestQuery(query, expected);
    }
    
    @Test
    public void test5() throws Exception {
        String query = "WINNER=='blue' && TEAM=='gold' && NAME=='gold-1' && (((_Bounded_ = true) && (POINTS > 4 && POINTS <= 5)) || ((_Bounded_ = true) && (POINTS > 0 && POINTS < 2)))";
        String expected = "WINNER == 'blue' && ((((_Bounded_ = true) && (TEAM_NAME_POINTS > 'gold,gold-1,4' && TEAM_NAME_POINTS <= 'gold,gold-1,5')) && ((_Eval_ = true) && (TEAM == 'gold' && NAME == 'gold-1' && ((_Bounded_ = true) && (POINTS > 4 && POINTS <= 5))))) || (((_Bounded_ = true) && (TEAM_NAME_POINTS > 'gold,gold-1,0' && TEAM_NAME_POINTS < 'gold,gold-1,2')) && ((_Eval_ = true) && (TEAM == 'gold' && NAME == 'gold-1' && ((_Bounded_ = true) && (POINTS > 0 && POINTS < 2))))))";
        runTestQuery(query, expected);
    }
    
    @Test
    public void test6() throws Exception {
        String query = "WINNER == 'blue' && (TEAM=='gold' || NAME=='gold-1' || ((_Bounded_ = true) && (POINTS > 10 && POINTS <= 11)))";
        String expected = "WINNER == 'blue' && (TEAM == 'gold' || NAME == 'gold-1' || ((_Bounded_ = true) && (POINTS > 10 && POINTS <= 11)))";
        runTestQuery(query, expected);
    }
    
    @Test
    public void test7a() throws Exception {
        String query = "WINNER == 'blue' && TEAM=='gold' && ( NAME=='gold-1' || NAME=='gold-2' && ((_Bounded_ = true) && (POINTS > 10 && POINTS <= 11)))";
        String expected = "WINNER == 'blue' && ((TEAM == 'gold' && NAME == 'gold-1') || (((_Bounded_ = true) && (TEAM_NAME_POINTS > 'gold,gold-2,10' && TEAM_NAME_POINTS <= 'gold,gold-2,11')) && ((_Eval_ = true) && (TEAM == 'gold' && NAME == 'gold-2' && ((_Bounded_ = true) && (POINTS > 10 && POINTS <= 11))))))";
        runTestQuery(query, expected);
    }
    
    @Test
    public void test7aa() throws Exception {
        ShardQueryConfiguration myConf = new ShardQueryConfiguration();
        myConf.setCompositeToFieldMap(compositeToFieldMap);
        myConf.setCompositeFieldSeparators(compositeFieldSeparators);
        
        Set<String> indexedFields = Sets.newHashSet("MAKE", "COLOR", "WHEELS", "TEAM", "NAME", "POINTS");
        
        String query = "WINNER == 'blue' && COLOR == 'red' && TEAM=='gold' && ( WHEELS == 4 || NOM=='gold-1' || NOM=='gold-2' && ((_Bounded_ = true) && (POINTS > 10 && POINTS <= 11)))";
        String expected = "WINNER == 'blue' && ((TEAM == 'gold' && COLOR_WHEELS == 'red,4') || (COLOR == 'red' && NOM == 'gold-2' && ((_Bounded_ = true) && (TEAM_POINTS > 'gold,10' && TEAM_POINTS <= 'gold,11')) && ((_Eval_ = true) && (TEAM == 'gold' && ((_Bounded_ = true) && (POINTS > 10 && POINTS <= 11))))) || (COLOR == 'red' && TEAM == 'gold' && NOM == 'gold-1'))";
        runTestQuery(query, expected, indexedFields, myConf);
    }
    
    @Test
    public void test7b() throws Exception {
        String query = "WINNER == 'blue' && ((_Bounded_ = true) && (TEAM >= 'gold' && TEAM <= 'silver')) && ((_Bounded_ = true) && (NAME >= 'gold-1' && NAME <= 'gold-2')) && ((_Bounded_ = true) && (POINTS > 10 && POINTS <= 11))";
        String expected = "WINNER == 'blue' && ((_Bounded_ = true) && (TEAM >= 'gold' && TEAM <= 'silver')) && ((_Bounded_ = true) && (NAME >= 'gold-1' && NAME <= 'gold-2')) && ((_Bounded_ = true) && (POINTS > 10 && POINTS <= 11))";
        runTestQuery(query, expected);
    }
    
    @Test
    public void test7c() throws Exception {
        String query = "((_Bounded_ = true) && (TEAM >= 'gold' && TEAM <= 'silver')) && ((_Bounded_ = true) && (POINTS > 10 && POINTS <= 11))";
        String expected = "((_Bounded_ = true) && (TEAM >= 'gold' && TEAM <= 'silver')) && ((_Bounded_ = true) && (POINTS > 10 && POINTS <= 11))";
        runTestQuery(query, expected);
    }
    
    @Test
    public void test7d() throws Exception {
        String query = "((_Bounded_ = true) && (TEAM >= 'gold' && TEAM <= 'silver')) && ((_Bounded_ = true) && (POINTS > 10 && POINTS <= 11))";
        String expected = "((_Bounded_ = true) && (TEAM_POINTS > 'gold,10' && TEAM_POINTS <= 'silver,11')) && ((_Eval_ = true) && (((_Bounded_ = true) && (TEAM >= 'gold' && TEAM <= 'silver')) && ((_Bounded_ = true) && (POINTS > 10 && POINTS <= 11))))";
        
        conf.getFieldToDiscreteIndexTypes().put("TEAM", new MockDiscreteIndexType());
        
        runTestQuery(query, expected);
    }
    
    @Test
    public void test8() throws Exception {
        String query = "COLOR =~ '.*ed' && (WHEELS == '4' || WHEELS == '+aE4') && (MAKE_COLOR == 'honda' || MAKE == 'honda') && TYPE == 'truck'";
        String expected = "TYPE == 'truck' && (WHEELS == '4' || WHEELS == '+aE4') && ((COLOR =~ '.*ed' && MAKE_COLOR == 'honda') || (MAKE_COLOR =~ 'honda,.*ed' && ((_Eval_ = true) && (MAKE == 'honda' && COLOR =~ '.*ed'))))";
        runTestQuery(query, expected);
    }
    
    @Test
    public void test9() throws Exception {
        String query = "WINNER == 'blue' && TEAM == 'gold' && NAME != 'gold-1' && (((_Bounded_ = true) && (POINTS > 4 && POINTS <= 5)) || ((_Bounded_ = true) && (POINTS > 0 && POINTS < 2)))";
        String expected = "WINNER == 'blue' && NAME != 'gold-1' && ((((_Bounded_ = true) && (TEAM_POINTS > 'gold,4' && TEAM_POINTS <= 'gold,5')) && ((_Eval_ = true) && (TEAM == 'gold' && ((_Bounded_ = true) && (POINTS > 4 && POINTS <= 5))))) || (((_Bounded_ = true) && (TEAM_POINTS > 'gold,0' && TEAM_POINTS < 'gold,2')) && ((_Eval_ = true) && (TEAM == 'gold' && ((_Bounded_ = true) && (POINTS > 0 && POINTS < 2))))))";
        runTestQuery(query, expected);
    }
    
    @Test
    public void test10() throws Exception {
        String query = "WINNER == 'blue' && TEAM == 'gold' && !(((_Bounded_ = true) && (POINTS > 4 && POINTS <= 5)) || ((_Bounded_ = true) && (POINTS > 0 && POINTS < 2)))";
        String expected = "WINNER == 'blue' && TEAM == 'gold' && !(((_Bounded_ = true) && (POINTS > 4 && POINTS <= 5)) || ((_Bounded_ = true) && (POINTS > 0 && POINTS < 2)))";
        runTestQuery(query, expected);
    }
    
    @Test
    public void test11() throws Exception {
        String query = "WINNER == 'blue' && TEAM == 'gold' && NAME != 'gold-1' && (((_Bounded_ = true) && (POINTS > 4 && POINTS <= 5)) || ((_Bounded_ = true) && (POINTS > 0 && POINTS < 2)))";
        String expected = "WINNER == 'blue' && NAME != 'gold-1' && ((((_Bounded_ = true) && (TEAM_POINTS > 'gold,4' && TEAM_POINTS <= 'gold,5')) && ((_Eval_ = true) && (TEAM == 'gold' && ((_Bounded_ = true) && (POINTS > 4 && POINTS <= 5))))) || (((_Bounded_ = true) && (TEAM_POINTS > 'gold,0' && TEAM_POINTS < 'gold,2')) && ((_Eval_ = true) && (TEAM == 'gold' && ((_Bounded_ = true) && (POINTS > 0 && POINTS < 2))))))";
        runTestQuery(query, expected);
    }
    
    @Test
    public void test12() throws Exception {
        ShardQueryConfiguration conf = new ShardQueryConfiguration();
        
        Multimap<String,String> compositeToFieldMap = LinkedListMultimap.create();
        
        compositeToFieldMap.put("GEO", "GEO");
        compositeToFieldMap.put("GEO", "WKT_BYTE_LENGTH");
        conf.setCompositeToFieldMap(compositeToFieldMap);
        
        Map<String,String> compositeToSeparatorMap = new HashMap<>();
        compositeToSeparatorMap.put("GEO", ",");
        conf.setCompositeFieldSeparators(compositeToSeparatorMap);
        
        Set<String> indexedFields = new HashSet<>();
        indexedFields.add("GEO");
        
        conf.getFieldToDiscreteIndexTypes().put("GEO", new GeometryType());
        
        String query = "(((_Bounded_ = true) && (GEO >= '1f0155640000000000' && GEO <= '1f01556bffffffffff')) || GEO == '00' || ((_Bounded_ = true) && (GEO >= '0100' && GEO <= '0103'))) && ((_Bounded_ = true) && (WKT_BYTE_LENGTH >= '"
                        + Normalizer.NUMBER_NORMALIZER.normalize("0") + "' && WKT_BYTE_LENGTH <= '" + Normalizer.NUMBER_NORMALIZER.normalize("12345") + "'))";
        String expected = "((((_Bounded_ = true) && (GEO >= '1f0155640000000000,+AE0' && GEO <= '1f01556bffffffffff,+eE1.2345')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO >= '1f0155640000000000' && GEO <= '1f01556bffffffffff')) && ((_Bounded_ = true) && (WKT_BYTE_LENGTH >= '+AE0' && WKT_BYTE_LENGTH <= '+eE1.2345'))))) || (((_Bounded_ = true) && (GEO >= '00,+AE0' && GEO <= '00,+eE1.2345')) && ((_Eval_ = true) && (GEO == '00' && ((_Bounded_ = true) && (WKT_BYTE_LENGTH >= '+AE0' && WKT_BYTE_LENGTH <= '+eE1.2345'))))) || (((_Bounded_ = true) && (GEO >= '0100,+AE0' && GEO <= '0103,+eE1.2345')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO >= '0100' && GEO <= '0103')) && ((_Bounded_ = true) && (WKT_BYTE_LENGTH >= '+AE0' && WKT_BYTE_LENGTH <= '+eE1.2345'))))))";
        
        runTestQuery(query, expected, indexedFields, conf);
    }
    
    @Test
    public void test13() throws Exception {
        ShardQueryConfiguration conf = new ShardQueryConfiguration();
        
        Multimap<String,String> compositeToFieldMap = LinkedListMultimap.create();
        
        compositeToFieldMap.put("GEO", "GEO");
        compositeToFieldMap.put("GEO", "WKT_BYTE_LENGTH");
        conf.setCompositeToFieldMap(compositeToFieldMap);
        
        Map<String,String> compositeToSeparatorMap = new HashMap<>();
        compositeToSeparatorMap.put("GEO", ",");
        conf.setCompositeFieldSeparators(compositeToSeparatorMap);
        
        Set<String> indexedFields = new HashSet<>();
        indexedFields.add("GEO");
        
        conf.getFieldToDiscreteIndexTypes().put("GEO", new GeometryType());
        
        String query = "((_Bounded_ = true) && (GEO >= '0100' && GEO <= '0103')) && WKT_BYTE_LENGTH >= '" + Normalizer.NUMBER_NORMALIZER.normalize("0") + "'";
        String expected = "((_Bounded_ = true) && (GEO >= '0100,+AE0' && GEO < '0104')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO >= '0100' && GEO <= '0103')) && WKT_BYTE_LENGTH >= '+AE0'))";
        
        runTestQuery(query, expected, indexedFields, conf);
    }
    
    @Test
    public void test14() throws Exception {
        ShardQueryConfiguration conf = new ShardQueryConfiguration();
        
        Multimap<String,String> compositeToFieldMap = LinkedListMultimap.create();
        
        compositeToFieldMap.put("GEO", "GEO");
        compositeToFieldMap.put("GEO", "WKT_BYTE_LENGTH");
        conf.setCompositeToFieldMap(compositeToFieldMap);
        
        Map<String,String> compositeToSeparatorMap = new HashMap<>();
        compositeToSeparatorMap.put("GEO", ",");
        conf.setCompositeFieldSeparators(compositeToSeparatorMap);
        
        Set<String> indexedFields = new HashSet<>();
        indexedFields.add("GEO");
        
        conf.getFieldToDiscreteIndexTypes().put("GEO", new GeometryType());
        
        String query = "((_Bounded_ = true) && (GEO >= '0100' && GEO <= '0103')) && WKT_BYTE_LENGTH <= '" + Normalizer.NUMBER_NORMALIZER.normalize("12345")
                        + "'";
        String expected = "((_Bounded_ = true) && (GEO >= '0100' && GEO <= '0103,+eE1.2345')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO >= '0100' && GEO <= '0103')) && WKT_BYTE_LENGTH <= '+eE1.2345'))";
        
        runTestQuery(query, expected, indexedFields, conf);
    }
    
    @Test
    public void test15() throws Exception {
        ShardQueryConfiguration conf = new ShardQueryConfiguration();
        
        Multimap<String,String> compositeToFieldMap = LinkedListMultimap.create();
        
        compositeToFieldMap.put("GEO", "GEO");
        compositeToFieldMap.put("GEO", "WKT_BYTE_LENGTH");
        conf.setCompositeToFieldMap(compositeToFieldMap);
        
        Set<String> indexedFields = new HashSet<>();
        indexedFields.add("GEO");
        
        conf.getFieldToDiscreteIndexTypes().put("GEO", new GeometryType());
        
        String query = "GEO >= '0100' && WKT_BYTE_LENGTH <= '" + Normalizer.NUMBER_NORMALIZER.normalize("12345") + "'";
        String expected = "GEO >= '0100' && WKT_BYTE_LENGTH <= '" + Normalizer.NUMBER_NORMALIZER.normalize("12345") + "'";
        
        runTestQuery(query, expected, indexedFields, conf);
    }
    
    @Test
    public void test16() throws Exception {
        ShardQueryConfiguration conf = new ShardQueryConfiguration();
        
        Multimap<String,String> compositeToFieldMap = LinkedListMultimap.create();
        
        compositeToFieldMap.put("GEO", "GEO");
        compositeToFieldMap.put("GEO", "WKT_BYTE_LENGTH");
        conf.setCompositeToFieldMap(compositeToFieldMap);
        
        Set<String> indexedFields = new HashSet<>();
        indexedFields.add("GEO");
        
        conf.getFieldToDiscreteIndexTypes().put("GEO", new GeometryType());
        
        String query = "GEO <= '0103' && WKT_BYTE_LENGTH >= '" + Normalizer.NUMBER_NORMALIZER.normalize("12345") + "'";
        String expected = "GEO <= '0103' && WKT_BYTE_LENGTH >= '" + Normalizer.NUMBER_NORMALIZER.normalize("12345") + "'";
        
        runTestQuery(query, expected, indexedFields, conf);
    }
    
    @Test
    public void test17() throws Exception {
        ShardQueryConfiguration conf = new ShardQueryConfiguration();
        
        Multimap<String,String> compositeToFieldMap = LinkedListMultimap.create();
        
        compositeToFieldMap.put("GEO", "GEO");
        compositeToFieldMap.put("GEO", "WKT_BYTE_LENGTH");
        conf.setCompositeToFieldMap(compositeToFieldMap);
        
        Map<String,String> compositeToSeparatorMap = new HashMap<>();
        compositeToSeparatorMap.put("GEO", ",");
        conf.setCompositeFieldSeparators(compositeToSeparatorMap);
        
        Set<String> indexedFields = new HashSet<>();
        indexedFields.add("GEO");
        
        conf.getFieldToDiscreteIndexTypes().put("GEO", new GeometryType());
        
        String query = "(((((_Bounded_ = true) && (GEO >= '0202' && GEO <= '020d')))) || ((((_Bounded_ = true) && (GEO >= '030a' && GEO <= '0335')))) || ((((_Bounded_ = true) && (GEO >= '0428' && GEO <= '0483')))) || ((((_Bounded_ = true) && (GEO >= '0500aa' && GEO <= '050355')))) || ((((_Bounded_ = true) && (GEO >= '1f0aaaaaaaaaaaaaaa' && GEO <= '1f36c71c71c71c71c7'))))) && (((_Bounded_ = true) && (WKT_BYTE_LENGTH >= '+AE0' && WKT_BYTE_LENGTH <= '"
                        + Normalizer.NUMBER_NORMALIZER.normalize("12345") + "')))";
        String expected = "((((((_Bounded_ = true) && (GEO >= '0202,+AE0' && GEO <= '020d,+eE1.2345')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO >= '0202' && GEO <= '020d')) && ((_Bounded_ = true) && (WKT_BYTE_LENGTH >= '+AE0' && WKT_BYTE_LENGTH <= '+eE1.2345'))))))) || (((((_Bounded_ = true) && (GEO >= '030a,+AE0' && GEO <= '0335,+eE1.2345')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO >= '030a' && GEO <= '0335')) && ((_Bounded_ = true) && (WKT_BYTE_LENGTH >= '+AE0' && WKT_BYTE_LENGTH <= '+eE1.2345'))))))) || (((((_Bounded_ = true) && (GEO >= '0428,+AE0' && GEO <= '0483,+eE1.2345')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO >= '0428' && GEO <= '0483')) && ((_Bounded_ = true) && (WKT_BYTE_LENGTH >= '+AE0' && WKT_BYTE_LENGTH <= '+eE1.2345'))))))) || (((((_Bounded_ = true) && (GEO >= '0500aa,+AE0' && GEO <= '050355,+eE1.2345')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO >= '0500aa' && GEO <= '050355')) && ((_Bounded_ = true) && (WKT_BYTE_LENGTH >= '+AE0' && WKT_BYTE_LENGTH <= '+eE1.2345'))))))) || (((((_Bounded_ = true) && (GEO >= '1f0aaaaaaaaaaaaaaa,+AE0' && GEO <= '1f36c71c71c71c71c7,+eE1.2345')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO >= '1f0aaaaaaaaaaaaaaa' && GEO <= '1f36c71c71c71c71c7')) && ((_Bounded_ = true) && (WKT_BYTE_LENGTH >= '+AE0' && WKT_BYTE_LENGTH <= '+eE1.2345'))))))))";
        
        runTestQuery(query, expected, indexedFields, conf);
    }
    
    // Composite Range testing with an overloaded composite field
    @Test
    public void test18a() throws Exception {
        ShardQueryConfiguration conf = new ShardQueryConfiguration();
        
        Multimap<String,String> compositeToFieldMap = LinkedListMultimap.create();
        compositeToFieldMap.put("GEO", "GEO");
        compositeToFieldMap.put("GEO", "WKT");
        conf.setCompositeToFieldMap(compositeToFieldMap);
        
        Map<String,String> compositeToSeparatorMap = new HashMap<>();
        compositeToSeparatorMap.put("GEO", ",");
        conf.setCompositeFieldSeparators(compositeToSeparatorMap);
        
        Set<String> indexedFields = new HashSet<>();
        indexedFields.add("GEO");
        
        conf.getFieldToDiscreteIndexTypes().put("GEO", new GeometryType());
        
        String upperBound = Normalizer.NUMBER_NORMALIZER.normalize("12345");
        
        // COMPOSITE QUERY AGAINST THE COMPOSITE INDEX
        // if incrementing/decrementing is an option
        // NOTE: Because we are combining two ranges, our bounds will already include some unwanted composite terms.
        // Those will be taken care of via a combination of accumulo iterator filtering against the shard index,
        // and field index filtering against the field index within the index iterators.
        // GE to GE -> GE
        // GE to GT -> GT
        // GT to GT -> increment base, GT
        // GT to GE -> increment base, GE
        // GT to EQ -> increment base, GE
        // EQ to GT -> GT
        // EQ to GE -> GE
        // LE to LE -> LE
        // LE to LT -> LT
        // LT to LT -> decrement base, LT
        // LT to LE -> decrement base, LE
        // LT to EQ -> decrement base, LE
        // EQ to LT -> LT
        // EQ to LE -> LE
        
        // NON-COMPOSITE QUERY AGAINST AN OVERLOADED COMPOSITE INDEX
        // if incrementing/decrementing is an option
        // NOTE: The proposed solutions only work IFF the underlying data is truly a unicode string
        // GE -> GE
        // GT -> increment base, GE
        // LE -> increment base, LT
        // LT -> LT
        // EQ -> EQ convert to range, lower bound -> inclusive term, upper bound -> exclusive incremented term
        // e.g. GEO == '0202'
        // GEO >= '0202' && GEO < '0203'
        
        String query, expected;
        // GE to GE, use GE
        // LE to LE, use LE
        query = "((_Bounded_ = true) && (GEO >= '0202' && GEO <= '020d')) && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT <= '" + upperBound + "'))";
        expected = "((_Bounded_ = true) && (GEO >= '0202,+AE0' && GEO <= '020d,+eE1.2345')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO >= '0202' && GEO <= '020d')) && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT <= '+eE1.2345'))))";
        runTestQuery(query, expected, indexedFields, conf);
        // GT to GE, increment fixed term, use GE
        // LE to LE, use LE
        query = "((_Bounded_ = true) && (GEO > '0202' && GEO <= '020d')) && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT <= '" + upperBound + "'))";
        expected = "((_Bounded_ = true) && (GEO >= '0203,+AE0' && GEO <= '020d,+eE1.2345')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO > '0202' && GEO <= '020d')) && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT <= '+eE1.2345'))))";
        runTestQuery(query, expected, indexedFields, conf);
        // GE to GE, use GE
        // LT to LE, decrement fixed term, use LE
        query = "((_Bounded_ = true) && (GEO >= '0202' && GEO < '020d')) && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT <= '" + upperBound + "'))";
        expected = "((_Bounded_ = true) && (GEO >= '0202,+AE0' && GEO <= '020c,+eE1.2345')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO >= '0202' && GEO < '020d')) && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT <= '+eE1.2345'))))";
        runTestQuery(query, expected, indexedFields, conf);
        // GT to GE, increment fixed term, use GE
        // LT to LE, decrement fixed term, use LE
        query = "((_Bounded_ = true) && (GEO > '0202' && GEO < '020d') )&& ((_Bounded_ = true) && (WKT >= '+AE0' && WKT <= '" + upperBound + "'))";
        expected = "((_Bounded_ = true) && (GEO >= '0203,+AE0' && GEO <= '020c,+eE1.2345')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO > '0202' && GEO < '020d')) && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT <= '+eE1.2345'))))";
        runTestQuery(query, expected, indexedFields, conf);
        
        // GE to GT, use GT
        // LE to LE, use LE
        query = "((_Bounded_ = true) && (GEO >= '0202' && GEO <= '020d')) && ((_Bounded_ = true) && (WKT > '+AE0' && WKT <= '" + upperBound + "'))";
        expected = "((_Bounded_ = true) && (GEO > '0202,+AE0' && GEO <= '020d,+eE1.2345')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO >= '0202' && GEO <= '020d')) && ((_Bounded_ = true) && (WKT > '+AE0' && WKT <= '+eE1.2345'))))";
        runTestQuery(query, expected, indexedFields, conf);
        // GT to GT, increment fixed term, use GT
        // LE to LE, use LE
        query = "((_Bounded_ = true) && (GEO > '0202' && GEO <= '020d')) && ((_Bounded_ = true) && (WKT > '+AE0' && WKT <= '" + upperBound + "'))";
        expected = "((_Bounded_ = true) && (GEO > '0203,+AE0' && GEO <= '020d,+eE1.2345')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO > '0202' && GEO <= '020d')) && ((_Bounded_ = true) && (WKT > '+AE0' && WKT <= '+eE1.2345'))))";
        runTestQuery(query, expected, indexedFields, conf);
        // GE to GT, use GT
        // LT to LE, decrement fixed term, use LE
        query = "((_Bounded_ = true) && (GEO >= '0202' && GEO < '020d')) && ((_Bounded_ = true) && (WKT > '+AE0' && WKT <= '" + upperBound + "'))";
        expected = "((_Bounded_ = true) && (GEO > '0202,+AE0' && GEO <= '020c,+eE1.2345')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO >= '0202' && GEO < '020d')) && ((_Bounded_ = true) && (WKT > '+AE0' && WKT <= '+eE1.2345'))))";
        runTestQuery(query, expected, indexedFields, conf);
        // GT to GT, increment base, use GT
        // LT to LE, decrement fixed term, use LE
        query = "((_Bounded_ = true) && (GEO > '0202' && GEO < '020d')) && ((_Bounded_ = true) && (WKT > '+AE0' && WKT <= '" + upperBound + "'))";
        expected = "((_Bounded_ = true) && (GEO > '0203,+AE0' && GEO <= '020c,+eE1.2345')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO > '0202' && GEO < '020d')) && ((_Bounded_ = true) && (WKT > '+AE0' && WKT <= '+eE1.2345'))))";
        runTestQuery(query, expected, indexedFields, conf);
        
        // GE to GE, use GE
        // LE to LT, use LT
        query = "((_Bounded_ = true) && (GEO >= '0202' && GEO <= '020d')) && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT < '" + upperBound + "'))";
        expected = "((_Bounded_ = true) && (GEO >= '0202,+AE0' && GEO < '020d,+eE1.2345')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO >= '0202' && GEO <= '020d')) && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT < '+eE1.2345'))))";
        runTestQuery(query, expected, indexedFields, conf);
        // GT to GE, increment fixed term, use GE
        // LE to LT, use LT
        query = "((_Bounded_ = true) && (GEO > '0202' && GEO <= '020d')) && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT < '" + upperBound + "'))";
        expected = "((_Bounded_ = true) && (GEO >= '0203,+AE0' && GEO < '020d,+eE1.2345')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO > '0202' && GEO <= '020d')) && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT < '+eE1.2345'))))";
        runTestQuery(query, expected, indexedFields, conf);
        // GE to GE, use GE
        // LT to LT, decrement fixed term, use LT
        query = "((_Bounded_ = true) && (GEO >= '0202' && GEO < '020d')) && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT < '" + upperBound + "'))";
        expected = "((_Bounded_ = true) && (GEO >= '0202,+AE0' && GEO < '020c,+eE1.2345')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO >= '0202' && GEO < '020d')) && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT < '+eE1.2345'))))";
        runTestQuery(query, expected, indexedFields, conf);
        // GT to GE, increment fixed term, use GE
        // LT to LT, decrement fixed term, use LT
        query = "((_Bounded_ = true) && (GEO > '0202' && GEO < '020d')) && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT < '" + upperBound + "'))";
        expected = "((_Bounded_ = true) && (GEO >= '0203,+AE0' && GEO < '020c,+eE1.2345')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO > '0202' && GEO < '020d')) && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT < '+eE1.2345'))))";
        runTestQuery(query, expected, indexedFields, conf);
        
        // GE to GT, use GT
        // LE to LT, use LT
        query = "((_Bounded_ = true) && (GEO >= '0202' && GEO <= '020d')) && ((_Bounded_ = true) && (WKT > '+AE0' && WKT < '" + upperBound + "'))";
        expected = "((_Bounded_ = true) && (GEO > '0202,+AE0' && GEO < '020d,+eE1.2345')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO >= '0202' && GEO <= '020d')) && ((_Bounded_ = true) && (WKT > '+AE0' && WKT < '+eE1.2345'))))";
        runTestQuery(query, expected, indexedFields, conf);
        // GT to GT, increment fixed term, use GT
        // LE to LT, use LT
        query = "((_Bounded_ = true) && (GEO > '0202' && GEO <= '020d')) && ((_Bounded_ = true) && (WKT > '+AE0' && WKT < '" + upperBound + "'))";
        expected = "((_Bounded_ = true) && (GEO > '0203,+AE0' && GEO < '020d,+eE1.2345')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO > '0202' && GEO <= '020d')) && ((_Bounded_ = true) && (WKT > '+AE0' && WKT < '+eE1.2345'))))";
        runTestQuery(query, expected, indexedFields, conf);
        // GE to GT, use GT
        // LT to LT, decrement fixed term, use LT
        query = "((_Bounded_ = true) && (GEO >= '0202' && GEO < '020d')) && ((_Bounded_ = true) && (WKT > '+AE0' && WKT < '" + upperBound + "'))";
        expected = "((_Bounded_ = true) && (GEO > '0202,+AE0' && GEO < '020c,+eE1.2345')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO >= '0202' && GEO < '020d')) && ((_Bounded_ = true) && (WKT > '+AE0' && WKT < '+eE1.2345'))))";
        runTestQuery(query, expected, indexedFields, conf);
        // GT to GT, increment fixed term, use GT
        // LT to LT, decrement fixed term, use LT
        query = "((_Bounded_ = true) && (GEO > '0202' && GEO < '020d')) && ((_Bounded_ = true) && (WKT > '+AE0' && WKT < '" + upperBound + "'))";
        expected = "((_Bounded_ = true) && (GEO > '0203,+AE0' && GEO < '020c,+eE1.2345')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO > '0202' && GEO < '020d')) && ((_Bounded_ = true) && (WKT > '+AE0' && WKT < '+eE1.2345'))))";
        runTestQuery(query, expected, indexedFields, conf);
        
        // EQ to GE, use GE
        // EQ to LE, use LE
        query = "(GEO == '0202') && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT <= '" + upperBound + "'))";
        expected = "((_Bounded_ = true) && (GEO >= '0202,+AE0' && GEO <= '0202,+eE1.2345')) && ((_Eval_ = true) && (GEO == '0202' && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT <= '+eE1.2345'))))";
        runTestQuery(query, expected, indexedFields, conf);
        // EQ to GE, use GE
        // EQ to LT, use LT
        query = "(GEO == '0202') && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT < '" + upperBound + "'))";
        expected = "((_Bounded_ = true) && (GEO >= '0202,+AE0' && GEO < '0202,+eE1.2345')) && ((_Eval_ = true) && (GEO == '0202' && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT < '+eE1.2345'))))";
        runTestQuery(query, expected, indexedFields, conf);
        // EQ to GT, use GT
        // EQ to LE, use LE
        query = "(GEO == '0202') && ((_Bounded_ = true) && (WKT > '+AE0' && WKT <= '" + upperBound + "'))";
        expected = "((_Bounded_ = true) && (GEO > '0202,+AE0' && GEO <= '0202,+eE1.2345')) && ((_Eval_ = true) && (GEO == '0202' && ((_Bounded_ = true) && (WKT > '+AE0' && WKT <= '+eE1.2345'))))";
        runTestQuery(query, expected, indexedFields, conf);
        // EQ to GT, use GT
        // EQ to LT, use LT
        query = "(GEO == '0202') && ((_Bounded_ = true) && (WKT > '+AE0' && WKT < '" + upperBound + "'))";
        expected = "((_Bounded_ = true) && (GEO > '0202,+AE0' && GEO < '0202,+eE1.2345')) && ((_Eval_ = true) && (GEO == '0202' && ((_Bounded_ = true) && (WKT > '+AE0' && WKT < '+eE1.2345'))))";
        runTestQuery(query, expected, indexedFields, conf);
        
        // EQ, convert to range [keep base - use GE, increment base - use LT]
        query = "GEO == '0202'";
        expected = "((_Bounded_ = true) && (GEO >= '0202' && GEO < '0203'))";
        runTestQuery(query, expected, indexedFields, conf);
        
        // Unbounded range w/ composite term
        query = "GEO >= '0202' && WKT < '" + upperBound + "'";
        expected = "GEO >= '0202' && WKT < '" + upperBound + "'";
        runTestQuery(query, expected, indexedFields, conf);
        
        query = "GEO >= '0202' && WKT > '" + upperBound + "'";
        expected = "GEO >= '0202' && WKT > '" + upperBound + "'";
        runTestQuery(query, expected, indexedFields, conf);
        
        query = "GEO <= '0202' && WKT < '" + upperBound + "'";
        expected = "GEO <= '0202' && WKT < '" + upperBound + "'";
        runTestQuery(query, expected, indexedFields, conf);
        
        query = "GEO <= '0202' && WKT > '" + upperBound + "'";
        expected = "GEO <= '0202' && WKT > '" + upperBound + "'";
        runTestQuery(query, expected, indexedFields, conf);
        
        // Unbounded range w/out composite term
        query = "GEO >= '0202'";
        expected = "GEO >= '0202'";
        runTestQuery(query, expected, indexedFields, conf);
        
        query = "GEO > '0202'";
        expected = "GEO > '0202'";
        runTestQuery(query, expected, indexedFields, conf);
        
        query = "GEO <= '0202'";
        expected = "GEO <= '0202'";
        runTestQuery(query, expected, indexedFields, conf);
        
        query = "GEO < '0202'";
        expected = "GEO < '0202'";
        runTestQuery(query, expected, indexedFields, conf);
    }
    
    // Composite Range testing with an overloaded composite field against legacy data
    @Test
    public void test18b() throws Exception {
        ShardQueryConfiguration conf = new ShardQueryConfiguration();
        conf.setBeginDate(new Date(0));
        conf.setEndDate(new Date(TimeUnit.DAYS.toMillis(30)));
        
        Multimap<String,String> compositeToFieldMap = LinkedListMultimap.create();
        
        compositeToFieldMap.put("GEO", "GEO");
        compositeToFieldMap.put("GEO", "WKT");
        conf.setCompositeToFieldMap(compositeToFieldMap);
        
        Map<String,String> compositeToSeparatorMap = new HashMap<>();
        compositeToSeparatorMap.put("GEO", ",");
        conf.setCompositeFieldSeparators(compositeToSeparatorMap);
        
        Set<String> indexedFields = new HashSet<>();
        indexedFields.add("GEO");
        
        conf.getFieldToDiscreteIndexTypes().put("GEO", new GeometryType());
        
        Map<String,Date> compositeWithOldDataMap = new HashMap<>();
        compositeWithOldDataMap.put("GEO", new Date(TimeUnit.DAYS.toMillis(15)));
        conf.setCompositeTransitionDates(compositeWithOldDataMap);
        
        String upperBound = Normalizer.NUMBER_NORMALIZER.normalize("12345");
        
        // COMPOSITE QUERY AGAINST THE COMPOSITE INDEX
        // if incrementing/decrementing is an option
        // NOTE: Because we are combining two ranges, our bounds will already include some unwanted composite terms.
        // Those will be taken care of via a combination of accumulo iterator filtering against the shard index,
        // and field index filtering against the field index within the index iterators.
        // GE to GE -> GE
        // GE to GT -> GT
        // GT to GT -> increment base, GT
        // GT to GE -> increment base, GE
        // GT to EQ -> increment base, GE
        // EQ to GT -> GT
        // EQ to GE -> GE
        // LE to LE -> LE
        // LE to LT -> LT
        // LT to LT -> decrement base, LT
        // LT to LE -> decrement base, LE
        // LT to EQ -> decrement base, LE
        // EQ to LT -> LT
        // EQ to LE -> LE
        
        // NON-COMPOSITE QUERY AGAINST AN OVERLOADED COMPOSITE INDEX
        // if incrementing/decrementing is an option
        // NOTE: The proposed solutions only work IFF the underlying data is truly a unicode string
        // GE -> GE
        // GT -> increment base, GE
        // LE -> increment base, LT
        // LT -> LT
        // EQ -> EQ convert to range, lower bound -> inclusive term, upper bound -> exclusive incremented term
        // e.g. GEO == '0202'
        // GEO >= '0202' && GEO < '0203'
        
        String query, expected;
        // GE to GE, use GE
        // LE to LE, use LE
        query = "((_Bounded_ = true) && (GEO >= '0202' && GEO <= '020d')) && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT <= '" + upperBound + "'))";
        expected = "((_Bounded_ = true) && (GEO >= '0202' && GEO <= '020d,+eE1.2345')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO >= '0202' && GEO <= '020d')) && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT <= '+eE1.2345'))))";
        runTestQuery(query, expected, indexedFields, conf);
        // GT to GE, increment fixed term, use GE
        // LE to LE, use LE
        query = "((_Bounded_ = true) && (GEO > '0202' && GEO <= '020d')) && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT <= '" + upperBound + "'))";
        expected = "((_Bounded_ = true) && (GEO >= '0203' && GEO <= '020d,+eE1.2345')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO > '0202' && GEO <= '020d')) && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT <= '+eE1.2345'))))";
        runTestQuery(query, expected, indexedFields, conf);
        // GE to GE, use GE
        // LT to LE, decrement fixed term, use LE
        query = "((_Bounded_ = true) && (GEO >= '0202' && GEO < '020d')) && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT <= '" + upperBound + "'))";
        expected = "((_Bounded_ = true) && (GEO >= '0202' && GEO <= '020c,+eE1.2345')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO >= '0202' && GEO < '020d')) && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT <= '+eE1.2345'))))";
        runTestQuery(query, expected, indexedFields, conf);
        // GT to GE, increment fixed term, use GE
        // LT to LE, decrement fixed term, use LE
        query = "((_Bounded_ = true) && (GEO > '0202' && GEO < '020d')) && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT <= '" + upperBound + "'))";
        expected = "((_Bounded_ = true) && (GEO >= '0203' && GEO <= '020c,+eE1.2345')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO > '0202' && GEO < '020d')) && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT <= '+eE1.2345'))))";
        runTestQuery(query, expected, indexedFields, conf);
        
        // GE to GT, use GT
        // LE to LE, use LE
        query = "((_Bounded_ = true) && (GEO >= '0202' && GEO <= '020d')) && ((_Bounded_ = true) && (WKT > '+AE0' && WKT <= '" + upperBound + "'))";
        expected = "((_Bounded_ = true) && (GEO >= '0202' && GEO <= '020d,+eE1.2345')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO >= '0202' && GEO <= '020d')) && ((_Bounded_ = true) && (WKT > '+AE0' && WKT <= '+eE1.2345'))))";
        runTestQuery(query, expected, indexedFields, conf);
        // GT to GT, increment fixed term, use GT
        // LE to LE, use LE
        query = "((_Bounded_ = true) && (GEO > '0202' && GEO <= '020d')) && ((_Bounded_ = true) && (WKT > '+AE0' && WKT <= '" + upperBound + "'))";
        expected = "((_Bounded_ = true) && (GEO >= '0203' && GEO <= '020d,+eE1.2345')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO > '0202' && GEO <= '020d')) && ((_Bounded_ = true) && (WKT > '+AE0' && WKT <= '+eE1.2345'))))";
        runTestQuery(query, expected, indexedFields, conf);
        // GE to GT, use GT
        // LT to LE, decrement fixed term, use LE
        query = "((_Bounded_ = true) && (GEO >= '0202' && GEO < '020d')) && ((_Bounded_ = true) && (WKT > '+AE0' && WKT <= '" + upperBound + "'))";
        expected = "((_Bounded_ = true) && (GEO >= '0202' && GEO <= '020c,+eE1.2345')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO >= '0202' && GEO < '020d')) && ((_Bounded_ = true) && (WKT > '+AE0' && WKT <= '+eE1.2345'))))";
        runTestQuery(query, expected, indexedFields, conf);
        // GT to GT, increment base, use GT
        // LT to LE, decrement fixed term, use LE
        query = "((_Bounded_ = true) && (GEO > '0202' && GEO < '020d')) && ((_Bounded_ = true) && (WKT > '+AE0' && WKT <= '" + upperBound + "'))";
        expected = "((_Bounded_ = true) && (GEO >= '0203' && GEO <= '020c,+eE1.2345')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO > '0202' && GEO < '020d')) && ((_Bounded_ = true) && (WKT > '+AE0' && WKT <= '+eE1.2345'))))";
        runTestQuery(query, expected, indexedFields, conf);
        
        // GE to GE, use GE
        // LE to LT, use LT
        query = "((_Bounded_ = true) && (GEO >= '0202' && GEO <= '020d')) && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT < '" + upperBound + "'))";
        expected = "((_Bounded_ = true) && (GEO >= '0202' && GEO < '020d,+eE1.2345')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO >= '0202' && GEO <= '020d')) && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT < '+eE1.2345'))))";
        runTestQuery(query, expected, indexedFields, conf);
        // GT to GE, increment fixed term, use GE
        // LE to LT, use LT
        query = "((_Bounded_ = true) && (GEO > '0202' && GEO <= '020d')) && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT < '" + upperBound + "'))";
        expected = "((_Bounded_ = true) && (GEO >= '0203' && GEO < '020d,+eE1.2345')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO > '0202' && GEO <= '020d')) && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT < '+eE1.2345'))))";
        runTestQuery(query, expected, indexedFields, conf);
        // GE to GE, use GE
        // LT to LT, decrement fixed term, use LT
        query = "((_Bounded_ = true) && (GEO >= '0202' && GEO < '020d')) && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT < '" + upperBound + "'))";
        expected = "((_Bounded_ = true) && (GEO >= '0202' && GEO < '020c,+eE1.2345')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO >= '0202' && GEO < '020d')) && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT < '+eE1.2345'))))";
        runTestQuery(query, expected, indexedFields, conf);
        // GT to GE, increment fixed term, use GE
        // LT to LT, decrement fixed term, use LT
        query = "((_Bounded_ = true) && (GEO > '0202' && GEO < '020d')) && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT < '" + upperBound + "'))";
        expected = "((_Bounded_ = true) && (GEO >= '0203' && GEO < '020c,+eE1.2345')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO > '0202' && GEO < '020d')) && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT < '+eE1.2345'))))";
        runTestQuery(query, expected, indexedFields, conf);
        
        // GE to GT, use GT
        // LE to LT, use LT
        query = "((_Bounded_ = true) && (GEO >= '0202' && GEO <= '020d')) && ((_Bounded_ = true) && (WKT > '+AE0' && WKT < '" + upperBound + "'))";
        expected = "((_Bounded_ = true) && (GEO >= '0202' && GEO < '020d,+eE1.2345')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO >= '0202' && GEO <= '020d')) && ((_Bounded_ = true) && (WKT > '+AE0' && WKT < '+eE1.2345'))))";
        runTestQuery(query, expected, indexedFields, conf);
        // GT to GT, increment fixed term, use GT
        // LE to LT, use LT
        query = "((_Bounded_ = true) && (GEO > '0202' && GEO <= '020d')) && ((_Bounded_ = true) && (WKT > '+AE0' && WKT < '" + upperBound + "'))";
        expected = "((_Bounded_ = true) && (GEO >= '0203' && GEO < '020d,+eE1.2345')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO > '0202' && GEO <= '020d')) && ((_Bounded_ = true) && (WKT > '+AE0' && WKT < '+eE1.2345'))))";
        runTestQuery(query, expected, indexedFields, conf);
        // GE to GT, use GT
        // LT to LT, decrement fixed term, use LT
        query = "((_Bounded_ = true) && (GEO >= '0202' && GEO < '020d')) && ((_Bounded_ = true) && (WKT > '+AE0' && WKT < '" + upperBound + "'))";
        expected = "((_Bounded_ = true) && (GEO >= '0202' && GEO < '020c,+eE1.2345')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO >= '0202' && GEO < '020d')) && ((_Bounded_ = true) && (WKT > '+AE0' && WKT < '+eE1.2345'))))";
        runTestQuery(query, expected, indexedFields, conf);
        // GT to GT, increment fixed term, use GT
        // LT to LT, decrement fixed term, use LT
        query = "((_Bounded_ = true) && (GEO > '0202' && GEO < '020d')) && ((_Bounded_ = true) && (WKT > '+AE0' && WKT < '" + upperBound + "'))";
        expected = "((_Bounded_ = true) && (GEO >= '0203' && GEO < '020c,+eE1.2345')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO > '0202' && GEO < '020d')) && ((_Bounded_ = true) && (WKT > '+AE0' && WKT < '+eE1.2345'))))";
        runTestQuery(query, expected, indexedFields, conf);
        
        // EQ to GE, use GE
        // EQ to LE, use LE
        query = "(GEO == '0202') && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT <= '" + upperBound + "'))";
        expected = "((_Bounded_ = true) && (GEO >= '0202' && GEO <= '0202,+eE1.2345')) && ((_Eval_ = true) && (GEO == '0202' && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT <= '+eE1.2345'))))";
        runTestQuery(query, expected, indexedFields, conf);
        // EQ to GE, use GE
        // EQ to LT, use LT
        query = "(GEO == '0202') && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT < '" + upperBound + "'))";
        expected = "((_Bounded_ = true) && (GEO >= '0202' && GEO < '0202,+eE1.2345')) && ((_Eval_ = true) && (GEO == '0202' && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT < '+eE1.2345'))))";
        runTestQuery(query, expected, indexedFields, conf);
        // EQ to GT, use GT
        // EQ to LE, use LE
        query = "(GEO == '0202') && ((_Bounded_ = true) && (WKT > '+AE0' && WKT <= '" + upperBound + "'))";
        expected = "((_Bounded_ = true) && (GEO >= '0202' && GEO <= '0202,+eE1.2345')) && ((_Eval_ = true) && (GEO == '0202' && ((_Bounded_ = true) && (WKT > '+AE0' && WKT <= '+eE1.2345'))))";
        runTestQuery(query, expected, indexedFields, conf);
        // EQ to GT, use GT
        // EQ to LT, use LT
        query = "(GEO == '0202') && ((_Bounded_ = true) && (WKT > '+AE0' && WKT < '" + upperBound + "'))";
        expected = "((_Bounded_ = true) && (GEO >= '0202' && GEO < '0202,+eE1.2345')) && ((_Eval_ = true) && (GEO == '0202' && ((_Bounded_ = true) && (WKT > '+AE0' && WKT < '+eE1.2345'))))";
        runTestQuery(query, expected, indexedFields, conf);
        
        // EQ, convert to range [keep base - use GE, increment base - use LT]
        query = "GEO == '0202'";
        expected = "((_Bounded_ = true) && (GEO >= '0202' && GEO < '0203'))";
        runTestQuery(query, expected, indexedFields, conf);
        
        // Unbounded range w/ composite term
        query = "GEO >= '0202' && WKT < '" + upperBound + "'";
        expected = "GEO >= '0202' && WKT < '" + upperBound + "'";
        runTestQuery(query, expected, indexedFields, conf);
        
        query = "GEO >= '0202' && WKT > '" + upperBound + "'";
        expected = "GEO >= '0202' && WKT > '" + upperBound + "'";
        runTestQuery(query, expected, indexedFields, conf);
        
        query = "GEO <= '0202' && WKT < '" + upperBound + "'";
        expected = "GEO <= '0202' && WKT < '" + upperBound + "'";
        runTestQuery(query, expected, indexedFields, conf);
        
        query = "GEO <= '0202' && WKT > '" + upperBound + "'";
        expected = "GEO <= '0202' && WKT > '" + upperBound + "'";
        runTestQuery(query, expected, indexedFields, conf);
        
        // Unbounded range w/out composite term
        query = "GEO >= '0202'";
        expected = "GEO >= '0202'";
        runTestQuery(query, expected, indexedFields, conf);
        
        query = "GEO > '0202'";
        expected = "GEO > '0202'";
        runTestQuery(query, expected, indexedFields, conf);
        
        query = "GEO <= '0202'";
        expected = "GEO <= '0202'";
        runTestQuery(query, expected, indexedFields, conf);
        
        query = "GEO < '0202'";
        expected = "GEO < '0202'";
        runTestQuery(query, expected, indexedFields, conf);
    }
    
    // Composite Range testing with a normal composite field
    @Test
    public void test19() throws Exception {
        ShardQueryConfiguration conf = new ShardQueryConfiguration();
        
        Multimap<String,String> compositeToFieldMap = LinkedListMultimap.create();
        
        compositeToFieldMap.put("GEO_WKT", "GEO");
        compositeToFieldMap.put("GEO_WKT", "WKT");
        conf.setCompositeToFieldMap(compositeToFieldMap);
        
        Map<String,String> compositeToSeparatorMap = new HashMap<>();
        compositeToSeparatorMap.put("GEO_WKT", ",");
        conf.setCompositeFieldSeparators(compositeToSeparatorMap);
        
        Set<String> indexedFields = new HashSet<>();
        indexedFields.add("GEO");
        
        conf.getFieldToDiscreteIndexTypes().put("GEO", new GeometryType());
        
        String upperBound = Normalizer.NUMBER_NORMALIZER.normalize("12345");
        
        // COMPOSITE QUERY AGAINST THE COMPOSITE INDEX
        // if incrementing/decrementing is an option
        // NOTE: Because we are combining two ranges, our bounds will already include some unwanted composite terms.
        // Those will be taken care of via a combination of accumulo iterator filtering against the shard index,
        // and field index filtering against the field index within the index iterators.
        // GE to GE -> GE
        // GE to GT -> GT
        // GT to GT -> increment base, GT
        // GT to GE -> increment base, GE
        // GT to EQ -> increment base, GE
        // EQ to GT -> GT
        // EQ to GE -> GE
        // LE to LE -> LE
        // LE to LT -> LT
        // LT to LT -> decrement base, LT
        // LT to LE -> decrement base, LE
        // LT to EQ -> decrement base, LE
        // EQ to LT -> LT
        // EQ to LE -> LE
        
        // NON-COMPOSITE QUERY AGAINST AN OVERLOADED COMPOSITE INDEX
        // if incrementing/decrementing is an option
        // NOTE: The proposed solutions only work IFF the underlying data is truly a unicode string
        // GE -> GE
        // GT -> increment base, GE
        // LE -> increment base, LT
        // LT -> LT
        // EQ -> EQ convert to range, lower bound -> inclusive term, upper bound -> exclusive incremented term
        // e.g. GEO == '0202'
        // GEO >= '0202' && GEO < '0203'
        
        String query, expected;
        // GE to GE, use GE
        // LE to LE, use LE
        query = "((_Bounded_ = true) && (GEO >= '0202' && GEO <= '020d')) && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT <= '" + upperBound + "'))";
        expected = "((_Bounded_ = true) && (GEO_WKT >= '0202,+AE0' && GEO_WKT <= '020d,+eE1.2345')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO >= '0202' && GEO <= '020d')) && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT <= '+eE1.2345'))))";
        runTestQuery(query, expected, indexedFields, conf);
        // GT to GE, increment fixed term, use GE
        // LE to LE, use LE
        query = "((_Bounded_ = true) && (GEO > '0202' && GEO <= '020d')) && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT <= '" + upperBound + "'))";
        expected = "((_Bounded_ = true) && (GEO_WKT >= '0203,+AE0' && GEO_WKT <= '020d,+eE1.2345')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO > '0202' && GEO <= '020d')) && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT <= '+eE1.2345'))))";
        runTestQuery(query, expected, indexedFields, conf);
        // GE to GE, use GE
        // LT to LE, decrement fixed term, use LE
        query = "((_Bounded_ = true) && (GEO >= '0202' && GEO < '020d')) && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT <= '" + upperBound + "'))";
        expected = "((_Bounded_ = true) && (GEO_WKT >= '0202,+AE0' && GEO_WKT <= '020c,+eE1.2345')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO >= '0202' && GEO < '020d')) && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT <= '+eE1.2345'))))";
        runTestQuery(query, expected, indexedFields, conf);
        // GT to GE, increment fixed term, use GE
        // LT to LE, decrement fixed term, use LE
        query = "((_Bounded_ = true) && (GEO > '0202' && GEO < '020d')) && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT <= '" + upperBound + "'))";
        expected = "((_Bounded_ = true) && (GEO_WKT >= '0203,+AE0' && GEO_WKT <= '020c,+eE1.2345')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO > '0202' && GEO < '020d')) && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT <= '+eE1.2345'))))";
        runTestQuery(query, expected, indexedFields, conf);
        
        // GE to GT, use GT
        // LE to LE, use LE
        query = "((_Bounded_ = true) && (GEO >= '0202' && GEO <= '020d')) && ((_Bounded_ = true) && (WKT > '+AE0' && WKT <= '" + upperBound + "'))";
        expected = "((_Bounded_ = true) && (GEO_WKT > '0202,+AE0' && GEO_WKT <= '020d,+eE1.2345')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO >= '0202' && GEO <= '020d')) && ((_Bounded_ = true) && (WKT > '+AE0' && WKT <= '+eE1.2345'))))";
        runTestQuery(query, expected, indexedFields, conf);
        // GT to GT, increment fixed term, use GT
        // LE to LE, use LE
        query = "((_Bounded_ = true) && (GEO > '0202' && GEO <= '020d')) && ((_Bounded_ = true) && (WKT > '+AE0' && WKT <= '" + upperBound + "'))";
        expected = "((_Bounded_ = true) && (GEO_WKT > '0203,+AE0' && GEO_WKT <= '020d,+eE1.2345')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO > '0202' && GEO <= '020d')) && ((_Bounded_ = true) && (WKT > '+AE0' && WKT <= '+eE1.2345'))))";
        runTestQuery(query, expected, indexedFields, conf);
        // GE to GT, use GT
        // LT to LE, decrement fixed term, use LE
        query = "((_Bounded_ = true) && (GEO >= '0202' && GEO < '020d')) && ((_Bounded_ = true) && (WKT > '+AE0' && WKT <= '" + upperBound + "'))";
        expected = "((_Bounded_ = true) && (GEO_WKT > '0202,+AE0' && GEO_WKT <= '020c,+eE1.2345')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO >= '0202' && GEO < '020d')) && ((_Bounded_ = true) && (WKT > '+AE0' && WKT <= '+eE1.2345'))))";
        runTestQuery(query, expected, indexedFields, conf);
        // GT to GT, increment base, use GT
        // LT to LE, decrement fixed term, use LE
        query = "((_Bounded_ = true) && (GEO > '0202' && GEO < '020d')) && ((_Bounded_ = true) && (WKT > '+AE0' && WKT <= '" + upperBound + "'))";
        expected = "((_Bounded_ = true) && (GEO_WKT > '0203,+AE0' && GEO_WKT <= '020c,+eE1.2345')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO > '0202' && GEO < '020d')) && ((_Bounded_ = true) && (WKT > '+AE0' && WKT <= '+eE1.2345'))))";
        runTestQuery(query, expected, indexedFields, conf);
        
        // GE to GE, use GE
        // LE to LT, use LT
        query = "((_Bounded_ = true) && (GEO >= '0202' && GEO <= '020d')) && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT < '" + upperBound + "'))";
        expected = "((_Bounded_ = true) && (GEO_WKT >= '0202,+AE0' && GEO_WKT < '020d,+eE1.2345')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO >= '0202' && GEO <= '020d')) && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT < '+eE1.2345'))))";
        runTestQuery(query, expected, indexedFields, conf);
        // GT to GE, increment fixed term, use GE
        // LE to LT, use LT
        query = "((_Bounded_ = true) && (GEO > '0202' && GEO <= '020d')) && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT < '" + upperBound + "'))";
        expected = "((_Bounded_ = true) && (GEO_WKT >= '0203,+AE0' && GEO_WKT < '020d,+eE1.2345')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO > '0202' && GEO <= '020d')) && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT < '+eE1.2345'))))";
        runTestQuery(query, expected, indexedFields, conf);
        // GE to GE, use GE
        // LT to LT, decrement fixed term, use LT
        query = "((_Bounded_ = true) && (GEO >= '0202' && GEO < '020d')) && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT < '" + upperBound + "'))";
        expected = "((_Bounded_ = true) && (GEO_WKT >= '0202,+AE0' && GEO_WKT < '020c,+eE1.2345')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO >= '0202' && GEO < '020d')) && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT < '+eE1.2345'))))";
        runTestQuery(query, expected, indexedFields, conf);
        // GT to GE, increment fixed term, use GE
        // LT to LT, decrement fixed term, use LT
        query = "((_Bounded_ = true) && (GEO > '0202' && GEO < '020d')) && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT < '" + upperBound + "'))";
        expected = "((_Bounded_ = true) && (GEO_WKT >= '0203,+AE0' && GEO_WKT < '020c,+eE1.2345')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO > '0202' && GEO < '020d')) && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT < '+eE1.2345'))))";
        runTestQuery(query, expected, indexedFields, conf);
        
        // GE to GT, use GT
        // LE to LT, use LT
        query = "((_Bounded_ = true) && (GEO >= '0202' && GEO <= '020d')) && ((_Bounded_ = true) && (WKT > '+AE0' && WKT < '" + upperBound + "'))";
        expected = "((_Bounded_ = true) && (GEO_WKT > '0202,+AE0' && GEO_WKT < '020d,+eE1.2345')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO >= '0202' && GEO <= '020d')) && ((_Bounded_ = true) && (WKT > '+AE0' && WKT < '+eE1.2345'))))";
        runTestQuery(query, expected, indexedFields, conf);
        // GT to GT, increment fixed term, use GT
        // LE to LT, use LT
        query = "((_Bounded_ = true) && (GEO > '0202' && GEO <= '020d')) && ((_Bounded_ = true) && (WKT > '+AE0' && WKT < '" + upperBound + "'))";
        expected = "((_Bounded_ = true) && (GEO_WKT > '0203,+AE0' && GEO_WKT < '020d,+eE1.2345')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO > '0202' && GEO <= '020d')) && ((_Bounded_ = true) && (WKT > '+AE0' && WKT < '+eE1.2345'))))";
        runTestQuery(query, expected, indexedFields, conf);
        // GE to GT, use GT
        // LT to LT, decrement fixed term, use LT
        query = "((_Bounded_ = true) && (GEO >= '0202' && GEO < '020d')) && ((_Bounded_ = true) && (WKT > '+AE0' && WKT < '" + upperBound + "'))";
        expected = "((_Bounded_ = true) && (GEO_WKT > '0202,+AE0' && GEO_WKT < '020c,+eE1.2345')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO >= '0202' && GEO < '020d')) && ((_Bounded_ = true) && (WKT > '+AE0' && WKT < '+eE1.2345'))))";
        runTestQuery(query, expected, indexedFields, conf);
        // GT to GT, increment fixed term, use GT
        // LT to LT, decrement fixed term, use LT
        query = "((_Bounded_ = true) && (GEO > '0202' && GEO < '020d')) && ((_Bounded_ = true) && (WKT > '+AE0' && WKT < '" + upperBound + "'))";
        expected = "((_Bounded_ = true) && (GEO_WKT > '0203,+AE0' && GEO_WKT < '020c,+eE1.2345')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO > '0202' && GEO < '020d')) && ((_Bounded_ = true) && (WKT > '+AE0' && WKT < '+eE1.2345'))))";
        runTestQuery(query, expected, indexedFields, conf);
        
        // EQ to GE, use GE
        // EQ to LE, use LE
        query = "(GEO == '0202') && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT <= '" + upperBound + "'))";
        expected = "((_Bounded_ = true) && (GEO_WKT >= '0202,+AE0' && GEO_WKT <= '0202,+eE1.2345')) && ((_Eval_ = true) && (GEO == '0202' && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT <= '+eE1.2345'))))";
        runTestQuery(query, expected, indexedFields, conf);
        // EQ to GE, use GE
        // EQ to LT, use LT
        query = "(GEO == '0202') && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT < '" + upperBound + "'))";
        expected = "((_Bounded_ = true) && (GEO_WKT >= '0202,+AE0' && GEO_WKT < '0202,+eE1.2345')) && ((_Eval_ = true) && (GEO == '0202' && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT < '+eE1.2345'))))";
        runTestQuery(query, expected, indexedFields, conf);
        // EQ to GT, use GT
        // EQ to LE, use LE
        query = "(GEO == '0202') && ((_Bounded_ = true) && (WKT > '+AE0' && WKT <= '" + upperBound + "'))";
        expected = "((_Bounded_ = true) && (GEO_WKT > '0202,+AE0' && GEO_WKT <= '0202,+eE1.2345')) && ((_Eval_ = true) && (GEO == '0202' && ((_Bounded_ = true) && (WKT > '+AE0' && WKT <= '+eE1.2345'))))";
        runTestQuery(query, expected, indexedFields, conf);
        // EQ to GT, use GT
        // EQ to LT, use LT
        query = "(GEO == '0202') && ((_Bounded_ = true) && (WKT > '+AE0' && WKT < '" + upperBound + "'))";
        expected = "((_Bounded_ = true) && (GEO_WKT > '0202,+AE0' && GEO_WKT < '0202,+eE1.2345')) && ((_Eval_ = true) && (GEO == '0202' && ((_Bounded_ = true) && (WKT > '+AE0' && WKT < '+eE1.2345'))))";
        runTestQuery(query, expected, indexedFields, conf);
        
        // EQ, for non-overloaded, keep as-is
        query = "GEO == '0202'";
        expected = "GEO == '0202'";
        runTestQuery(query, expected, indexedFields, conf);
        
        // Unbounded range w/ composite term
        query = "GEO >= '0202' && WKT < '" + upperBound + "'";
        expected = "GEO >= '0202' && WKT < '" + upperBound + "'";
        runTestQuery(query, expected, indexedFields, conf);
        
        query = "GEO >= '0202' && WKT > '" + upperBound + "'";
        expected = "GEO >= '0202' && WKT > '" + upperBound + "'";
        runTestQuery(query, expected, indexedFields, conf);
        
        query = "GEO <= '0202' && WKT < '" + upperBound + "'";
        expected = "GEO <= '0202' && WKT < '" + upperBound + "'";
        runTestQuery(query, expected, indexedFields, conf);
        
        query = "GEO <= '0202' && WKT > '" + upperBound + "'";
        expected = "GEO <= '0202' && WKT > '" + upperBound + "'";
        runTestQuery(query, expected, indexedFields, conf);
        
        // Unbounded range w/out composite term
        query = "GEO >= '0202'";
        expected = "GEO >= '0202'";
        runTestQuery(query, expected, indexedFields, conf);
        
        query = "GEO > '0202'";
        expected = "GEO > '0202'";
        runTestQuery(query, expected, indexedFields, conf);
        
        query = "GEO <= '0202'";
        expected = "GEO <= '0202'";
        runTestQuery(query, expected, indexedFields, conf);
        
        query = "GEO < '0202'";
        expected = "GEO < '0202'";
        runTestQuery(query, expected, indexedFields, conf);
    }
    
    @Test
    public void test20() throws Exception {
        ShardQueryConfiguration conf = new ShardQueryConfiguration();
        
        Multimap<String,String> compositeToFieldMap = LinkedListMultimap.create();
        
        compositeToFieldMap.put("GEO", "GEO");
        compositeToFieldMap.put("GEO", "WKT");
        conf.setCompositeToFieldMap(compositeToFieldMap);
        
        Map<String,String> compositeToSeparatorMap = new HashMap<>();
        compositeToSeparatorMap.put("GEO", ",");
        conf.setCompositeFieldSeparators(compositeToSeparatorMap);
        
        Set<String> indexedFields = new HashSet<>();
        indexedFields.add("GEO");
        
        conf.getFieldToDiscreteIndexTypes().put("GEO", new GeometryType());
        
        String query = "(((((_Bounded_ = true) && (GEO >= '0202' && GEO <= '020d')))) || ((((_Bounded_ = true) && (GEO >= '030a' && GEO <= '0335')))) || ((((_Bounded_ = true) && (GEO >= '0428' && GEO <= '0483')))) || ((((_Bounded_ = true) && (GEO >= '0500aa' && GEO <= '050355')))) || ((((_Bounded_ = true) && (GEO >= '1f0aaaaaaaaaaaaaaa' && GEO <= '1f36c71c71c71c71c7')))))";
        String expected = "((((((_Bounded_ = true) && (GEO >= '0202' && GEO < '020e'))))) || (((((_Bounded_ = true) && (GEO >= '030a' && GEO < '0336'))))) || (((((_Bounded_ = true) && (GEO >= '0428' && GEO < '0484'))))) || (((((_Bounded_ = true) && (GEO >= '0500aa' && GEO < '050356'))))) || (((((_Bounded_ = true) && (GEO >= '1f0aaaaaaaaaaaaaaa' && GEO < '1f36c71c71c71c71c8'))))))";
        
        runTestQuery(query, expected, indexedFields, conf);
    }
    
    @Test
    public void test21() throws Exception {
        ShardQueryConfiguration conf = new ShardQueryConfiguration();
        
        Multimap<String,String> compositeToFieldMap = LinkedListMultimap.create();
        
        compositeToFieldMap.put("GEO", "GEO");
        compositeToFieldMap.put("GEO", "WKT");
        conf.setCompositeToFieldMap(compositeToFieldMap);
        
        Map<String,String> compositeToSeparatorMap = new HashMap<>();
        compositeToSeparatorMap.put("GEO", ",");
        conf.setCompositeFieldSeparators(compositeToSeparatorMap);
        
        Set<String> indexedFields = new HashSet<>();
        indexedFields.add("GEO");
        
        conf.getFieldToDiscreteIndexTypes().put("GEO", new GeometryType());
        
        String query = "(((((_Bounded_ = true) && (GEO >= '0202' && GEO <= '020d')))) || ((((_Bounded_ = true) && (GEO >= '030a' && GEO <= '0335')))) || ((((_Bounded_ = true) && (GEO >= '0428' && GEO <= '0483')))) || ((((_Bounded_ = true) && (GEO >= '0500aa' && GEO <= '050355')))) || ((((_Bounded_ = true) && (GEO >= '1f0aaaaaaaaaaaaaaa' && GEO <= '1f36c71c71c71c71c7'))))) && (((_Bounded_ = true) && (WKT >= '+AE0' && WKT < '+bE4')))";
        String expected = "((((((_Bounded_ = true) && (GEO >= '0202,+AE0' && GEO < '020d,+bE4')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO >= '0202' && GEO <= '020d')) && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT < '+bE4'))))))) || (((((_Bounded_ = true) && (GEO >= '030a,+AE0' && GEO < '0335,+bE4')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO >= '030a' && GEO <= '0335')) && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT < '+bE4'))))))) || (((((_Bounded_ = true) && (GEO >= '0428,+AE0' && GEO < '0483,+bE4')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO >= '0428' && GEO <= '0483')) && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT < '+bE4'))))))) || (((((_Bounded_ = true) && (GEO >= '0500aa,+AE0' && GEO < '050355,+bE4')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO >= '0500aa' && GEO <= '050355')) && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT < '+bE4'))))))) || (((((_Bounded_ = true) && (GEO >= '1f0aaaaaaaaaaaaaaa,+AE0' && GEO < '1f36c71c71c71c71c7,+bE4')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO >= '1f0aaaaaaaaaaaaaaa' && GEO <= '1f36c71c71c71c71c7')) && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT < '+bE4'))))))))";
        
        runTestQuery(query, expected, indexedFields, conf);
    }
    
    @Test
    public void test22() throws Exception {
        ShardQueryConfiguration conf = new ShardQueryConfiguration();
        
        Multimap<String,String> compositeToFieldMap = LinkedListMultimap.create();
        
        compositeToFieldMap.put("GEO", "GEO");
        compositeToFieldMap.put("GEO", "WKT");
        conf.setCompositeToFieldMap(compositeToFieldMap);
        
        Map<String,String> compositeToSeparatorMap = new HashMap<>();
        compositeToSeparatorMap.put("GEO", ",");
        conf.setCompositeFieldSeparators(compositeToSeparatorMap);
        
        Set<String> indexedFields = new HashSet<>();
        indexedFields.add("GEO");
        
        String query = "(((((_Bounded_ = true) && (GEO >= '0202' && GEO <= '020d')))) || ((((_Bounded_ = true) && (GEO >= '030a' && GEO <= '0335')))) || ((((_Bounded_ = true) && (GEO >= '0428' && GEO <= '0483')))) || ((((_Bounded_ = true) && (GEO >= '0500aa' && GEO <= '050355')))) || ((((_Bounded_ = true) && (GEO >= '1f0aaaaaaaaaaaaaaa' && GEO <= '1f36c71c71c71c71c7'))))) && (((_Bounded_ = true) && (WKT >= '+AE0' && WKT < '+bE4')))";
        String expected = "(((((_Bounded_ = true) && (GEO >= '0202' && GEO <= '020d')))) || ((((_Bounded_ = true) && (GEO >= '030a' && GEO <= '0335')))) || ((((_Bounded_ = true) && (GEO >= '0428' && GEO <= '0483')))) || ((((_Bounded_ = true) && (GEO >= '0500aa' && GEO <= '050355')))) || ((((_Bounded_ = true) && (GEO >= '1f0aaaaaaaaaaaaaaa' && GEO <= '1f36c71c71c71c71c7'))))) && (((_Bounded_ = true) && (WKT >= '+AE0' && WKT < '+bE4')))";
        
        runTestQuery(query, expected, indexedFields, conf);
    }
    
    @Test
    public void test23() throws Exception {
        ShardQueryConfiguration conf = new ShardQueryConfiguration();
        
        Multimap<String,String> compositeToFieldMap = LinkedListMultimap.create();
        
        compositeToFieldMap.put("GEO_WKT", "GEO");
        compositeToFieldMap.put("GEO_WKT", "WKT");
        conf.setCompositeToFieldMap(compositeToFieldMap);
        
        Map<String,String> compositeToSeparatorMap = new HashMap<>();
        compositeToSeparatorMap.put("GEO_WKT", ",");
        conf.setCompositeFieldSeparators(compositeToSeparatorMap);
        
        Set<String> indexedFields = new HashSet<>();
        indexedFields.add("GEO");
        
        conf.getFieldToDiscreteIndexTypes().put("GEO", new GeometryType());
        
        String query = "(((((_Bounded_ = true) && (GEO >= '0202' && GEO <= '020d')))) || ((((_Bounded_ = true) && (GEO >= '030a' && GEO <= '0335')))) || ((((_Bounded_ = true) && (GEO >= '0428' && GEO <= '0483')))) || ((((_Bounded_ = true) && (GEO >= '0500aa' && GEO <= '050355')))) || ((((_Bounded_ = true) && (GEO >= '1f0aaaaaaaaaaaaaaa' && GEO <= '1f36c71c71c71c71c7'))))) && (((_Bounded_ = true) && (WKT >= '+AE0' && WKT < '+bE4')))";
        String expected = "((((((_Bounded_ = true) && (GEO_WKT >= '0202,+AE0' && GEO_WKT < '020d,+bE4')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO >= '0202' && GEO <= '020d')) && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT < '+bE4'))))))) || (((((_Bounded_ = true) && (GEO_WKT >= '030a,+AE0' && GEO_WKT < '0335,+bE4')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO >= '030a' && GEO <= '0335')) && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT < '+bE4'))))))) || (((((_Bounded_ = true) && (GEO_WKT >= '0428,+AE0' && GEO_WKT < '0483,+bE4')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO >= '0428' && GEO <= '0483')) && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT < '+bE4'))))))) || (((((_Bounded_ = true) && (GEO_WKT >= '0500aa,+AE0' && GEO_WKT < '050355,+bE4')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO >= '0500aa' && GEO <= '050355')) && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT < '+bE4'))))))) || (((((_Bounded_ = true) && (GEO_WKT >= '1f0aaaaaaaaaaaaaaa,+AE0' && GEO_WKT < '1f36c71c71c71c71c7,+bE4')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO >= '1f0aaaaaaaaaaaaaaa' && GEO <= '1f36c71c71c71c71c7')) && ((_Bounded_ = true) && (WKT >= '+AE0' && WKT < '+bE4'))))))))";
        
        runTestQuery(query, expected, indexedFields, conf);
    }
    
    @Test
    public void test24() throws Exception {
        ShardQueryConfiguration conf = new ShardQueryConfiguration();
        
        Multimap<String,String> compositeToFieldMap = LinkedListMultimap.create();
        
        compositeToFieldMap.put("GEO_WKT", "GEO");
        compositeToFieldMap.put("GEO_WKT", "WKT");
        conf.setCompositeToFieldMap(compositeToFieldMap);
        
        Set<String> indexedFields = new HashSet<>();
        indexedFields.add("GEO");
        
        conf.getFieldToDiscreteIndexTypes().put("GEO", new GeometryType());
        
        String query = "(((((_Bounded_ = true) && (GEO >= '0202' && GEO <= '020d')))) || ((((_Bounded_ = true) && (GEO >= '030a' && GEO <= '0335')))) || ((((_Bounded_ = true) && (GEO >= '0428' && GEO <= '0483')))) || ((((_Bounded_ = true) && (GEO >= '0500aa' && GEO <= '050355')))) || ((((_Bounded_ = true) && (GEO >= '1f0aaaaaaaaaaaaaaa' && GEO <= '1f36c71c71c71c71c7')))))";
        String expected = "(((((_Bounded_ = true) && (GEO >= '0202' && GEO <= '020d')))) || ((((_Bounded_ = true) && (GEO >= '030a' && GEO <= '0335')))) || ((((_Bounded_ = true) && (GEO >= '0428' && GEO <= '0483')))) || ((((_Bounded_ = true) && (GEO >= '0500aa' && GEO <= '050355')))) || ((((_Bounded_ = true) && (GEO >= '1f0aaaaaaaaaaaaaaa' && GEO <= '1f36c71c71c71c71c7')))))";
        
        runTestQuery(query, expected, indexedFields, conf);
    }
    
    @Test
    public void test25() throws Exception {
        ShardQueryConfiguration conf = new ShardQueryConfiguration();
        
        Multimap<String,String> compositeToFieldMap = LinkedListMultimap.create();
        
        compositeToFieldMap.put("GEO_WKT", "GEO");
        compositeToFieldMap.put("GEO_WKT", "WKT");
        conf.setCompositeToFieldMap(compositeToFieldMap);
        
        Map<String,String> compositeToSeparatorMap = new HashMap<>();
        compositeToSeparatorMap.put("GEO_WKT", ",");
        conf.setCompositeFieldSeparators(compositeToSeparatorMap);
        
        Set<String> indexedFields = new HashSet<>();
        indexedFields.add("GEO");
        
        String query = "(((((_Bounded_ = true) && (GEO >= '0202' && GEO <= '020d')))) || ((((_Bounded_ = true) && (GEO >= '030a' && GEO <= '0335')))) || ((((_Bounded_ = true) && (GEO >= '0428' && GEO <= '0483')))) || ((((_Bounded_ = true) && (GEO >= '0500aa' && GEO <= '050355')))) || ((((_Bounded_ = true) && (GEO >= '1f0aaaaaaaaaaaaaaa' && GEO <= '1f36c71c71c71c71c7'))))) && (((_Bounded_ = true) && (WKT >= '+AE0' && WKT < '+bE4')))";
        String expected = "(((((_Bounded_ = true) && (GEO >= '0202' && GEO <= '020d')))) || ((((_Bounded_ = true) && (GEO >= '030a' && GEO <= '0335')))) || ((((_Bounded_ = true) && (GEO >= '0428' && GEO <= '0483')))) || ((((_Bounded_ = true) && (GEO >= '0500aa' && GEO <= '050355')))) || ((((_Bounded_ = true) && (GEO >= '1f0aaaaaaaaaaaaaaa' && GEO <= '1f36c71c71c71c71c7'))))) && (((_Bounded_ = true) && (WKT >= '+AE0' && WKT < '+bE4')))";
        
        runTestQuery(query, expected, indexedFields, conf);
    }
    
    @Test
    public void test26() throws Exception {
        ShardQueryConfiguration conf = new ShardQueryConfiguration();
        
        Multimap<String,String> compositeToFieldMap = LinkedListMultimap.create();
        
        compositeToFieldMap.put("GEO", "GEO");
        compositeToFieldMap.put("GEO", "WKT");
        conf.setCompositeToFieldMap(compositeToFieldMap);
        
        Map<String,String> compositeToSeparatorMap = new HashMap<>();
        compositeToSeparatorMap.put("GEO", ",");
        conf.setCompositeFieldSeparators(compositeToSeparatorMap);
        
        Set<String> indexedFields = new HashSet<>();
        indexedFields.add("GEO");
        
        conf.getFieldToDiscreteIndexTypes().put("GEO", new GeometryType());
        
        conf.setBeginDate(new Date(0));
        conf.setEndDate(new Date(TimeUnit.DAYS.toMillis(30)));
        
        Map<String,Date> compositeWithOldDataMap = new HashMap<>();
        compositeWithOldDataMap.put("GEO", new Date(TimeUnit.DAYS.toMillis(15)));
        conf.setCompositeTransitionDates(compositeWithOldDataMap);
        
        String normNum = Normalizer.NUMBER_NORMALIZER.normalize("55");
        
        String query = "(GEO == '0202' || (((_Bounded_ = true) && (GEO >= '030a' && GEO <= '0335')))) && WKT == '" + normNum + "'";
        String expected = "((((_Bounded_ = true) && (GEO >= '0202' && GEO <= '0202," + normNum + "')) && ((_Eval_ = true) && (GEO == '0202' && WKT == '"
                        + normNum + "'))) || ((((_Bounded_ = true) && (GEO >= '030a' && GEO <= '0335," + normNum
                        + "')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO >= '030a' && GEO <= '0335')) && WKT == '" + normNum + "')))))";
        
        runTestQuery(query, expected, indexedFields, conf);
    }
    
    @Test
    public void test27() throws Exception {
        ShardQueryConfiguration conf = new ShardQueryConfiguration();
        
        Multimap<String,String> compositeToFieldMap = LinkedListMultimap.create();
        
        compositeToFieldMap.put("GEO", "GEO");
        compositeToFieldMap.put("GEO", "WKT");
        conf.setCompositeToFieldMap(compositeToFieldMap);
        
        Map<String,String> compositeToSeparatorMap = new HashMap<>();
        compositeToSeparatorMap.put("GEO", ",");
        conf.setCompositeFieldSeparators(compositeToSeparatorMap);
        
        Set<String> indexedFields = new HashSet<>();
        indexedFields.add("GEO");
        
        conf.getFieldToDiscreteIndexTypes().put("GEO", new GeometryType());
        
        String normNum = Normalizer.NUMBER_NORMALIZER.normalize("55");
        
        String query = "(GEO == '0202' || (((_Bounded_ = true) && (GEO >= '030a' && GEO <= '0335')))) && WKT == '" + normNum + "'";
        String expected = "(GEO == '0202,+bE5.5' || ((((_Bounded_ = true) && (GEO >= '030a,+bE5.5' && GEO <= '0335,+bE5.5')) && ((_Eval_ = true) && (((_Bounded_ = true) && (GEO >= '030a' && GEO <= '0335')) && WKT == '+bE5.5')))))";
        
        runTestQuery(query, expected, indexedFields, conf);
    }
    
    @Test
    public void test28() throws Exception {
        ShardQueryConfiguration conf = new ShardQueryConfiguration();
        
        Multimap<String,String> compositeToFieldMap = LinkedListMultimap.create();
        
        compositeToFieldMap.put("GEO", "GEO");
        compositeToFieldMap.put("GEO", "WKT");
        conf.setCompositeToFieldMap(compositeToFieldMap);
        
        Map<String,String> compositeToSeparatorMap = new HashMap<>();
        compositeToSeparatorMap.put("GEO", ",");
        conf.setCompositeFieldSeparators(compositeToSeparatorMap);
        
        Set<String> indexedFields = new HashSet<>();
        indexedFields.add("GEO");
        
        conf.getFieldToDiscreteIndexTypes().put("GEO", new GeometryType());
        
        conf.setBeginDate(new Date(0));
        conf.setEndDate(new Date(TimeUnit.DAYS.toMillis(30)));
        
        Map<String,Date> compositeWithOldDataMap = new HashMap<>();
        compositeWithOldDataMap.put("GEO", new Date(TimeUnit.DAYS.toMillis(15)));
        conf.setCompositeTransitionDates(compositeWithOldDataMap);
        
        String normNum = Normalizer.NUMBER_NORMALIZER.normalize("55");
        
        String query = "(GEO == '0202' || GEO >= '030a') && WKT == '" + normNum + "'";
        String expected = "((WKT == '+bE5.5' && GEO >= '030a') || (((_Bounded_ = true) && (GEO >= '0202' && GEO <= '0202,+bE5.5')) && ((_Eval_ = true) && (GEO == '0202' && WKT == '+bE5.5'))))";
        
        runTestQuery(query, expected, indexedFields, conf);
    }
    
    @Test
    public void test29() throws Exception {
        ShardQueryConfiguration conf = new ShardQueryConfiguration();
        
        Multimap<String,String> compositeToFieldMap = LinkedListMultimap.create();
        
        compositeToFieldMap.put("GEO", "GEO");
        compositeToFieldMap.put("GEO", "WKT");
        conf.setCompositeToFieldMap(compositeToFieldMap);
        
        Map<String,String> compositeToSeparatorMap = new HashMap<>();
        compositeToSeparatorMap.put("GEO", ",");
        conf.setCompositeFieldSeparators(compositeToSeparatorMap);
        
        Set<String> indexedFields = new HashSet<>();
        indexedFields.add("GEO");
        
        conf.getFieldToDiscreteIndexTypes().put("GEO", new GeometryType());
        
        String query = "(GEO == '0202' || GEO >= '030a') && WKT == '+bE5.5'";
        String expected = "((WKT == '+bE5.5' && GEO >= '030a') || GEO == '0202,+bE5.5')";
        
        runTestQuery(query, expected, indexedFields, conf);
    }
    
    // this is testing that distribution of anded nodes into an or node is working
    @Test
    public void test30() throws Exception {
        String query = "MAKE == 'london' && ((CODE == 'ita' || CODE == 'iTa') || COLOR == 'missouri' || NUM == '+cE1')";
        String expected = "((MAKE == 'london' && (CODE == 'ita' || CODE == 'iTa' || NUM == '+cE1')) || MAKE_COLOR == 'london,missouri')";
        
        runTestQuery(query, expected);
    }
    
    void runTestQuery(String query, String expected) throws ParseException {
        runTestQuery(query, expected, INDEX_FIELDS, conf);
    }
    
    void runTestQuery(String query, String expected, Set<String> indexedFields, ShardQueryConfiguration conf) throws ParseException {
        ASTJexlScript original = JexlASTHelper.parseJexlQuery(query);
        
        MockMetadataHelper helper = new MockMetadataHelper();
        helper.setIndexedFields(indexedFields);
        
        ASTJexlScript expand = FunctionIndexQueryExpansionVisitor.expandFunctions(conf, helper, DateIndexHelper.getInstance(), original);
        expand = ExpandCompositeTerms.expandTerms(conf, expand);
        
        // Verify the script is as expected, and has a valid lineage.
        JexlNodeAssert.assertThat(expand).isEqualTo(expected).hasValidLineage();
        
        // Verify the original script was not modified, and still has a valid lineage.
        JexlNodeAssert.assertThat(original).isEqualTo(query).hasValidLineage();
    }
    
    void workIt(String query) throws Exception {
        System.err.println("incoming:" + query);
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        showIt("original", script);
        script = ExpandCompositeTerms.expandTerms(conf, script);
        showIt("expanded composites", script);
    }
    
    void showIt(String message, ASTJexlScript script) {
        System.err.println(message);
        PrintingVisitor.printQuery(script);
        System.err.println(JexlStringBuildingVisitor.buildQuery(script));
        script = TreeFlatteningRebuildingVisitor.flatten(script);
        System.err.println("flattened:");
        PrintingVisitor.printQuery(script);
        System.err.println(JexlStringBuildingVisitor.buildQuery(script));
        System.err.println();
    }
    
    private static class MockDiscreteIndexType extends BaseType<String> implements DiscreteIndexType<String> {
        
        public MockDiscreteIndexType() {
            super(new NoOpNormalizer());
        }
        
        @Override
        public String incrementIndex(String index) {
            return index;
        }
        
        @Override
        public String decrementIndex(String index) {
            return index;
        }
        
        @Override
        public List<String> discretizeRange(String beginIndex, String endIndex) {
            return Arrays.asList(beginIndex, endIndex);
        }
        
        @Override
        public boolean producesFixedLengthRanges() {
            return true;
        }
        
        @Override
        public int compareTo(Type<String> o) {
            return 0;
        }
    }
}
