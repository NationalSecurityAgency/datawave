package datawave.query.postprocessing.tf;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import datawave.query.function.Equality;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.log4j.Logger;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;

import datawave.data.type.Type;
import datawave.query.attributes.Document;
import datawave.query.predicate.EventDataQueryFilter;
import datawave.query.util.Tuple2;
import datawave.query.util.Tuple3;
import datawave.query.util.TypeMetadata;

public class TFFactory {
    private static final Logger log = Logger.getLogger(TFFactory.class);
    
    public static com.google.common.base.Function<Tuple2<Key,Document>,Tuple3<Key,Document,Map<String,Object>>> getFunction(ASTJexlScript query,
                    Set<String> contentExpansionFields, Set<String> termFrequencyFields, TypeMetadata typeMetadata, Equality equality,
                    EventDataQueryFilter evaluationFilter, SortedKeyValueIterator<Key,Value> sourceCopy, Set<String> tfIndexOnlyFields) {
        
        Multimap<String,Class<? extends Type<?>>> fieldMappings = LinkedListMultimap.create();
        for (Entry<String,String> dataType : typeMetadata.fold().entries()) {
            String dataTypeName = dataType.getValue();
            
            try {
                fieldMappings.put(dataType.getKey(), (Class<? extends Type<?>>) Class.forName(dataTypeName).asSubclass(Type.class));
            } catch (ClassNotFoundException e) {
                log.warn("Skipping instantiating a " + dataTypeName + " for " + dataType.getKey() + " because the class was not found.", e);
            }
            
        }
        
        return getFunction(query, contentExpansionFields, termFrequencyFields, fieldMappings, equality, evaluationFilter, sourceCopy, tfIndexOnlyFields);
    }
    
    /**
     * Factory method for creating the TF function used for generating the map context.
     * 
     * @param query
     * @param dataTypes
     * @param sourceDeepCopy
     * @return
     */
    public static com.google.common.base.Function<Tuple2<Key,Document>,Tuple3<Key,Document,Map<String,Object>>> getFunction(ASTJexlScript query,
                    Set<String> contentExpansionFields, Set<String> termFrequencyFields, Multimap<String,Class<? extends Type<?>>> dataTypes,
                    Equality equality, EventDataQueryFilter evaluationFilter, SortedKeyValueIterator<Key,Value> sourceDeepCopy, Set<String> tfIndexOnlyFields) {
        
        Multimap<String,String> termFrequencyFieldValues = TermOffsetPopulator.getTermFrequencyFieldValues(query, contentExpansionFields, termFrequencyFields,
                        dataTypes);
        
        if (termFrequencyFieldValues.isEmpty()) {
            return new EmptyTermFrequencyFunction();
        } else {
            return new TermOffsetFunction(new TermOffsetPopulator(termFrequencyFieldValues, contentExpansionFields, evaluationFilter, sourceDeepCopy),
                            tfIndexOnlyFields);
        }
    }
}
