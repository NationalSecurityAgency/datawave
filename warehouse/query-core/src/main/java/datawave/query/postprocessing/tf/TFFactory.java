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
    
    public static com.google.common.base.Function<Tuple2<Key,Document>,Tuple3<Key,Document,Map<String,Object>>> getFunction(TermFrequencyConfig tfConfig) {
        
        Multimap<String,Class<? extends Type<?>>> fieldMappings = LinkedListMultimap.create();
        for (Entry<String,String> dataType : tfConfig.getTypeMetadata().fold().entries()) {
            String dataTypeName = dataType.getValue();
            
            try {
                fieldMappings.put(dataType.getKey(), (Class<? extends Type<?>>) Class.forName(dataTypeName).asSubclass(Type.class));
            } catch (ClassNotFoundException e) {
                log.warn("Skipping instantiating a " + dataTypeName + " for " + dataType.getKey() + " because the class was not found.", e);
            }
        }
        
        return getFunction(tfConfig, fieldMappings);
    }
    
    /**
     * Factory method for creating the TF function used for generating the map context.
     * 
     * @param tfConfig
     *            config object used to build this funciton
     * @param dataTypes
     *            a mapping of datatypes
     * @return a TermFrequency function
     */
    public static com.google.common.base.Function<Tuple2<Key,Document>,Tuple3<Key,Document,Map<String,Object>>> getFunction(TermFrequencyConfig tfConfig,
                    Multimap<String,Class<? extends Type<?>>> dataTypes) {
        
        Multimap<String,String> termFrequencyFieldValues = TermOffsetPopulator.getTermFrequencyFieldValues(tfConfig.getScript(),
                        tfConfig.getContentExpansionFields(), tfConfig.getTfFields(), dataTypes);
        
        if (termFrequencyFieldValues.isEmpty()) {
            return new EmptyTermFrequencyFunction();
        } else {
            // uses the original source copy
            TermOffsetPopulator offsetPopulator = new TermOffsetPopulator(termFrequencyFieldValues, tfConfig.getContentExpansionFields(),
                            tfConfig.getEvaluationFilter(), tfConfig.getSource());
            
            return new TermOffsetFunction(tfConfig, offsetPopulator);
        }
    }
}
