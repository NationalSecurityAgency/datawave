package datawave.query.postprocessing.tf;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import datawave.data.type.Type;
import datawave.query.attributes.Document;
import datawave.query.util.Tuple2;
import datawave.query.util.Tuple3;
import org.apache.accumulo.core.data.Key;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.Map.Entry;

public class TFFactory {
    private static final Logger log = Logger.getLogger(TFFactory.class);

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

        Multimap<String,Class<? extends Type<?>>> fieldMappings = LinkedListMultimap.create();
        for (Entry<String,String> dataType : config.getTypeMetadata().fold().entries()) {
            String dataTypeName = dataType.getValue();

            try {
                fieldMappings.put(dataType.getKey(), (Class<? extends Type<?>>) Class.forName(dataTypeName).asSubclass(Type.class));
            } catch (ClassNotFoundException e) {
                log.warn("Skipping instantiating a " + dataTypeName + " for " + dataType.getKey() + " because the class was not found.", e);
            }

        }

        return getFunction(config, fieldMappings);
    }

    /**
     * Factory method for creating the TF function used for generating the map context.
     *
     * @param config
     *            a {@link TermFrequencyConfig} object
     * @param dataTypes
     *            a MultiMap of data type names to their respective classes
     * @return a new {@link TermOffsetFunction}
     */
    public static com.google.common.base.Function<Tuple2<Key,Document>,Tuple3<Key,Document,Map<String,Object>>> getFunction(TermFrequencyConfig config,
                    Multimap<String,Class<? extends Type<?>>> dataTypes) {

        Multimap<String,String> termFrequencyFieldValues = TermOffsetPopulator.getTermFrequencyFieldValues(config.getScript(),
                        config.getContentExpansionFields(), config.getTfFields(), dataTypes);

        if (termFrequencyFieldValues.isEmpty()) {
            return new EmptyTermFrequencyFunction();
        } else {

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
}
