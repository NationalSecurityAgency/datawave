package datawave.query.postprocessing.tf;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.junit.jupiter.api.Test;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.query.attributes.Document;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.util.Tuple2;
import datawave.query.util.Tuple3;

public class TermOffsetFunctionTest {

    @Test
    public void testApplyWithLogLogAggregationPruneToZero() throws Exception {
        TermFrequencyConfig config = new TermFrequencyConfig();
        config.setScript(JexlASTHelper.parseAndFlattenJexlQuery("content:phrase(FOO, termOffsetMap, 'bar', 'baz')"));
        DocumentKeysFunction docKeyFunction = new DocumentKeysFunction(config);

        MyTermOffsetPopulator termOffsetPopulator = new MyTermOffsetPopulator(null, config);

        TermOffsetFunction termOffsetFunction = new TermOffsetFunction(termOffsetPopulator, Collections.emptySet(), docKeyFunction);
        Tuple3<Key,Document,Map<String,Object>> tuple = termOffsetFunction.apply(new Tuple2<>(new Key(), new Document()));
        assertEquals(new Key(), tuple.first());
        assertEquals(new Document(), tuple.second());
        assertEquals(new HashMap<String,Object>(), tuple.third());
    }

    // no-op
    private static class MyTermOffsetPopulator extends TermOffsetPopulator {

        public MyTermOffsetPopulator(Multimap<String,String> termFrequencyFieldValues, TermFrequencyConfig config) {
            super(termFrequencyFieldValues, config);
        }

        public Multimap<String,String> getTermFrequencyFieldValues() {
            return HashMultimap.create();
        }

        public Map<String,Object> getContextMap(Key docKey, Set<Key> keys, Set<String> fields) {
            return new HashMap<>();
        }

        public Document document() {
            return new Document();
        }
    }

}
