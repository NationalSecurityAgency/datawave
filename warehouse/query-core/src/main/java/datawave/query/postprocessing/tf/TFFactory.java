package datawave.query.postprocessing.tf;

import java.util.Map;

import org.apache.accumulo.core.data.Key;

import com.google.common.collect.Multimap;

import datawave.query.attributes.Document;
import datawave.query.util.Tuple2;
import datawave.query.util.Tuple3;

/**
 * Builds a {@link TermOffsetFunction} from a provided {@link TermFrequencyConfig}
 */
public class TFFactory {
    private TFFactory() {
        // private constructor for a static utility
    }

    /**
     * Construct a {@link TermOffsetFunction} from the provided {@link TermFrequencyConfig}.
     *
     * @param config
     *            a {@link TermFrequencyConfig}
     * @return a {@link TermOffsetFunction}
     */
    public static com.google.common.base.Function<Tuple2<Key,Document>,Tuple3<Key,Document,Map<String,Object>>> getFunction(TermFrequencyConfig config) {

        Multimap<String,String> termFrequencyFieldValues = TermOffsetPopulator.getTermFrequencyFieldValues(config.getScript(),
                        config.getContentExpansionFields(), config.getTfFields());

        if (termFrequencyFieldValues.isEmpty()) {
            return new EmptyTermFrequencyFunction();
        }

        DocumentKeysFunction docKeyFunction = null;

        if (config.isTld()) {
            docKeyFunction = new DocumentKeysFunction(config);
        }

        TermOffsetPopulator offsetPopulator = new TermOffsetPopulator(termFrequencyFieldValues, config);

        TermOffsetFunction function = new TermOffsetFunction(offsetPopulator, config.getTfFields(), docKeyFunction);
        function.setAggregationThreshold(config.getTfAggregationThreshold());
        return function;
    }
}
