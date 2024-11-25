package datawave.microservice.querymetric.config;

import static datawave.query.util.MetadataHelperFactory.ALL_AUTHS_PROPERTY;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.WebClient;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationModule;
import com.github.benmanes.caffeine.cache.CaffeineSpec;

import datawave.core.common.connection.AccumuloClientPool;
import datawave.core.query.result.event.DefaultResponseObjectFactory;
import datawave.marking.MarkingFunctions;
import datawave.microservice.http.converter.html.BannerProvider;
import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.Correlator;
import datawave.microservice.querymetric.QueryMetricFactory;
import datawave.microservice.querymetric.QueryMetricFactoryImpl;
import datawave.microservice.querymetric.QueryMetricOperations;
import datawave.microservice.querymetric.QueryMetricOperationsStats;
import datawave.microservice.querymetric.factory.QueryMetricQueryLogicFactory;
import datawave.microservice.querymetric.factory.QueryMetricResponseFactory;
import datawave.microservice.querymetric.function.QueryMetricConsumer;
import datawave.microservice.querymetric.handler.LocalShardTableQueryMetricHandler;
import datawave.microservice.querymetric.handler.QueryGeometryHandler;
import datawave.microservice.querymetric.handler.QueryMetricCombiner;
import datawave.microservice.querymetric.handler.RemoteShardTableQueryMetricHandler;
import datawave.microservice.querymetric.handler.ShardTableQueryMetricHandler;
import datawave.microservice.querymetric.handler.SimpleQueryGeometryHandler;
import datawave.microservice.security.util.DnUtils;
import datawave.query.language.builder.jexl.JexlTreeBuilder;
import datawave.query.language.functions.jexl.EvaluationOnly;
import datawave.query.language.functions.jexl.JexlQueryFunction;
import datawave.query.language.parser.jexl.LuceneToJexlQueryParser;
import datawave.query.util.DateIndexHelper;
import datawave.query.util.DateIndexHelperFactory;
import datawave.query.util.TypeMetadataHelper;
import datawave.security.authorization.JWTTokenHandler;
import datawave.webservice.query.result.event.ResponseObjectFactory;

@Configuration
@EnableConfigurationProperties({QueryMetricHandlerProperties.class, TimelyProperties.class})
public class QueryMetricHandlerConfiguration {
    
    @Value("${spring.application.name}")
    private String applicationName;
    
    @Bean
    public QueryMetricConsumer queryMetricSink(QueryMetricOperations queryMetricOperations, Correlator correlator, QueryMetricOperationsStats stats) {
        return new QueryMetricConsumer(queryMetricOperations, correlator, stats);
    }
    
    @Bean
    public ObjectMapper objectMapper(QueryMetricFactory metricFactory) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        mapper.enable(MapperFeature.USE_WRAPPER_NAME_AS_PROPERTY_NAME);
        SimpleModule module = new SimpleModule("BaseQueryMetricMapping");
        module.addAbstractTypeMapping(BaseQueryMetric.class, metricFactory.createMetric().getClass());
        mapper.registerModule(module);
        mapper.registerModule(new GuavaModule());
        mapper.registerModule(new JaxbAnnotationModule());
        return mapper;
    }
    
    @Bean
    public ResponseObjectFactory responseObjectFactory() {
        return new DefaultResponseObjectFactory();
    }
    
    @Bean
    @ConditionalOnMissingBean
    QueryMetricFactory queryMetricFactory() {
        return new QueryMetricFactoryImpl();
    }
    
    @Bean
    @ConditionalOnMissingBean
    public ShardTableQueryMetricHandler shardTableQueryMetricHandler(QueryMetricHandlerProperties queryMetricHandlerProperties,
                    @Qualifier("warehouse") AccumuloClientPool accumuloClientPool, QueryMetricQueryLogicFactory logicFactory, QueryMetricFactory metricFactory,
                    MarkingFunctions markingFunctions, QueryMetricCombiner queryMetricCombiner, LuceneToJexlQueryParser luceneToJexlQueryParser,
                    ResponseObjectFactory responseObjectFactory, WebClient.Builder webClientBuilder,
                    @Autowired(required = false) JWTTokenHandler jwtTokenHandler, DnUtils dnUtils, QueryMetricResponseFactory queryMetricResponseFactory) {
        ShardTableQueryMetricHandler handler;
        if (queryMetricHandlerProperties.isUseRemoteQuery()) {
            handler = new RemoteShardTableQueryMetricHandler(queryMetricHandlerProperties, accumuloClientPool, logicFactory, metricFactory, markingFunctions,
                            queryMetricCombiner, luceneToJexlQueryParser, responseObjectFactory, webClientBuilder, jwtTokenHandler, dnUtils);
        } else {
            handler = new LocalShardTableQueryMetricHandler(queryMetricHandlerProperties, accumuloClientPool, logicFactory, metricFactory, markingFunctions,
                            queryMetricCombiner, luceneToJexlQueryParser, dnUtils);
        }
        handler.setQueryMetricResponseFactory(queryMetricResponseFactory);
        return handler;
    }
    
    @Bean
    @ConditionalOnMissingBean
    public QueryMetricCombiner queryMetricCombiner() {
        return new QueryMetricCombiner();
    }
    
    @Bean
    @ConditionalOnMissingBean
    public QueryGeometryHandler geometryHandler(QueryMetricHandlerProperties queryMetricHandlerProperties,
                    QueryMetricResponseFactory queryMetricResponseFactory) {
        QueryGeometryHandler handler = new SimpleQueryGeometryHandler(queryMetricHandlerProperties);
        handler.setQueryMetricResponseFactory(queryMetricResponseFactory);
        return handler;
    }
    
    @Bean
    @ConditionalOnMissingBean
    public QueryMetricResponseFactory queryMetricResponseFactory(ObjectProvider<BannerProvider> bannerProvider) {
        return new QueryMetricResponseFactory(bannerProvider.getIfAvailable(), "/" + applicationName);
    }
    
    @Bean
    @ConditionalOnMissingBean
    public LuceneToJexlQueryParser luceneToJexlQueryParser() {
        LuceneToJexlQueryParser luceneToJexlQueryParser = new LuceneToJexlQueryParser();
        Set<String> skipTokenizedUnfieldedFields = new LinkedHashSet<>();
        skipTokenizedUnfieldedFields.add("DOMETA");
        luceneToJexlQueryParser.setSkipTokenizeUnfieldedFields(skipTokenizedUnfieldedFields);
        
        Map<String,JexlQueryFunction> allowedFunctions = new LinkedHashMap<>();
        for (JexlQueryFunction f : JexlTreeBuilder.DEFAULT_ALLOWED_FUNCTION_LIST) {
            allowedFunctions.put(f.getClass().getCanonicalName(), f);
        }
        // configure EvaluationOnly with this parser
        EvaluationOnly evaluationOnly = new EvaluationOnly();
        evaluationOnly.setParser(luceneToJexlQueryParser);
        allowedFunctions.put(EvaluationOnly.class.getCanonicalName(), evaluationOnly);
        luceneToJexlQueryParser.setAllowedFunctions(new ArrayList<>(allowedFunctions.values()));
        return luceneToJexlQueryParser;
    }
    
    // This bean is used via autowire in DateIndexHelper
    @Bean(name = "dateIndexHelperCacheManager")
    public CaffeineCacheManager dateIndexHelperCacheManager(QueryMetricHandlerProperties queryMetricHandlerProperties) {
        System.setProperty(ALL_AUTHS_PROPERTY, queryMetricHandlerProperties.getMetadataDefaultAuths());
        CaffeineCacheManager caffeineCacheManager = new CaffeineCacheManager();
        caffeineCacheManager.setCaffeineSpec(CaffeineSpec.parse("maximumSize=1000, expireAfterAccess=24h, expireAfterWrite=24h"));
        return caffeineCacheManager;
    }
    
    @Bean
    public DateIndexHelperFactory dateIndexHelperFactory() {
        DateIndexHelper dateIndexHelper = DateIndexHelper.getInstance();
        return new DateIndexHelperFactory() {
            @Override
            public DateIndexHelper createDateIndexHelper() {
                return dateIndexHelper;
            }
        };
    }
    
    @Bean
    @Qualifier("queryMetrics")
    public TypeMetadataHelper.Factory typeMetadataFactory(ApplicationContext context) {
        return new TypeMetadataHelper.Factory(context);
    }
}
