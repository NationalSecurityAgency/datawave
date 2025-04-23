package datawave.microservice.dictionary.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.jackson.Jackson2ObjectMapperBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import datawave.accumulo.util.security.UserAuthFunctions;
import datawave.marking.MarkingFunctions;
import datawave.microservice.config.accumulo.AccumuloProperties;
import datawave.microservice.config.web.DatawaveServerProperties;
import datawave.microservice.dictionary.data.DataDictionary;
import datawave.microservice.dictionary.data.DataDictionaryImpl;
import datawave.microservice.dictionary.edge.EdgeDictionary;
import datawave.microservice.dictionary.edge.EdgeDictionaryImpl;
import datawave.microservice.metadata.MetadataDescriptionsHelper;
import datawave.microservice.metadata.MetadataDescriptionsHelperFactory;
import datawave.query.util.MetadataHelperFactory;
import datawave.webservice.dictionary.data.DefaultDataDictionary;
import datawave.webservice.dictionary.data.DefaultDescription;
import datawave.webservice.dictionary.data.DefaultDictionaryField;
import datawave.webservice.dictionary.data.DefaultFields;
import datawave.webservice.dictionary.data.FieldsBase;
import datawave.webservice.metadata.DefaultMetadataField;

@Configuration
@EnableConfigurationProperties(DictionaryServiceProperties.class)
public class DictionaryServiceConfiguration {
    @Bean
    @Qualifier("warehouse")
    public AccumuloProperties warehouseAccumuloProperties(DictionaryServiceProperties dictionaryServiceProperties) {
        return dictionaryServiceProperties.getAccumuloProperties();
    }
    
    @Bean
    @ConditionalOnMissingBean
    public UserAuthFunctions userAuthFunctions() {
        return UserAuthFunctions.getInstance();
    }
    
    @Bean
    @Scope("prototype")
    @ConditionalOnMissingBean
    public MetadataDescriptionsHelper<DefaultDescription> metadataHelperWithDescriptions(MarkingFunctions markingFunctions,
                    ResponseObjectFactory<DefaultDescription,DefaultDataDictionary,DefaultMetadataField,DefaultDictionaryField,DefaultFields> responseObjectFactory) {
        return new MetadataDescriptionsHelper<>(markingFunctions, responseObjectFactory);
    }
    
    @Bean
    @ConditionalOnMissingBean
    public DataDictionary datawaveDataDictionary(MarkingFunctions markingFunctions,
                    ResponseObjectFactory<DefaultDescription,DefaultDataDictionary,DefaultMetadataField,DefaultDictionaryField,DefaultFields> responseObjectFactory,
                    MetadataHelperFactory metadataHelperFactory, MetadataDescriptionsHelperFactory<DefaultDescription> metadataDescriptionsHelperFactory) {
        return new DataDictionaryImpl(markingFunctions, responseObjectFactory, metadataHelperFactory, metadataDescriptionsHelperFactory);
    }
    
    @Bean
    @ConditionalOnMissingBean
    public EdgeDictionary datawaveEdgeDictionary(MetadataHelperFactory metadataHelperFactory) {
        return new EdgeDictionaryImpl(metadataHelperFactory);
    }
    
    @Bean
    @ConditionalOnMissingBean
    public ResponseObjectFactory<DefaultDescription,DefaultDataDictionary,DefaultMetadataField,DefaultDictionaryField,DefaultFields> responseObjectFactory(
                    DatawaveServerProperties datawaveServerProperties) {
        return new ResponseObjectFactory<DefaultDescription,DefaultDataDictionary,DefaultMetadataField,DefaultDictionaryField,DefaultFields>() {
            @Override
            public DefaultDataDictionary getDataDictionary() {
                return new DefaultDataDictionary(datawaveServerProperties.getCdnUri() + "webjars/jquery/",
                                datawaveServerProperties.getCdnUri() + "webjars/datatables/js/");
            }
            
            @Override
            public DefaultDescription getDescription() {
                return new DefaultDescription();
            }
            
            @Override
            public DefaultFields getFields() {
                return new DefaultFields();
            }
        };
    }
    
    /**
     * Provides a {@link Jackson2ObjectMapperBuilderCustomizer} that adds the mix-in {@link FieldsBaseMixIn} for {@link FieldsBase} objects. This is here to
     * override Jackson de-serialization behavior for methods that receive a {@link FieldsBase} as a request body. We cannot de-serialize the payload as an
     * abstract object, so instead override it to the provided concrete type. If you wish to override this to provide an alternate implementation of
     * {@link FieldsBase}, then you can simply re-define this bean to supply a different mix-in class. When re-defining, you must use the same name
     * `dictionaryJacksonCustomizer`.
     *
     * @return a {@link Jackson2ObjectMapperBuilderCustomizer} that adds the mix-in {@link FieldsBaseMixIn} for {@link FieldsBase} objects.
     */
    @Bean
    public Jackson2ObjectMapperBuilderCustomizer dictionaryJacksonCustomizer() {
        return c -> c.mixIn(FieldsBase.class, FieldsBaseMixIn.class);
    }
    
    /**
     * A Jackson mix-in to specify that {@link FieldsBase} parameters should be de-serialized as though the parameter were an instance of {@link DefaultFields}
     * instead.
     */
    @JsonDeserialize(as = DefaultFields.class)
    public static class FieldsBaseMixIn {}
}
