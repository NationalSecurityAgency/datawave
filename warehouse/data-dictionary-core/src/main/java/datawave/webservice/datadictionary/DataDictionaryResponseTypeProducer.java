package datawave.webservice.datadictionary;

import com.fasterxml.jackson.core.type.TypeReference;
import datawave.webservice.dictionary.data.DefaultDataDictionary;

import javax.enterprise.inject.Produces;

/**
 * A CDI producer bean that produces the response type that we will expect to receive from the Dictionary Service when retrieving the data dictionary. This type
 * could be overridden by providing an alternative producer for the case where the data dictionary returns some other type than {@link DefaultDataDictionary}.
 */
public class DataDictionaryResponseTypeProducer {
    @Produces
    @DataDictionaryType
    public TypeReference<DefaultDataDictionary> produceDataDictionaryType() {
        return new TypeReference<DefaultDataDictionary>() {};
    }
}
