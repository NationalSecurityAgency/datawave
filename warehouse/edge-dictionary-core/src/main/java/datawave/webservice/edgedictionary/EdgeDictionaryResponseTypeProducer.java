package datawave.webservice.edgedictionary;

import javax.enterprise.inject.Produces;

import com.fasterxml.jackson.core.type.TypeReference;

import datawave.webservice.dictionary.edge.DefaultEdgeDictionary;

/**
 * A CDI producer bean that produces the response type that we will expect to receive from the Dictionary Service when retrieving the edge dictionary. This type
 * could be overridden by providing an alternative producer for the case where the edge dictionary returns some other type than {@link DefaultEdgeDictionary}.
 */
public class EdgeDictionaryResponseTypeProducer {
    @Produces
    @EdgeDictionaryType
    public TypeReference<DefaultEdgeDictionary> produceEdgeDictionaryType() {
        return new TypeReference<DefaultEdgeDictionary>() {};
    }
}
