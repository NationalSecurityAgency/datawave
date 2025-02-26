package datawave.microservice.querymetric.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.autoconfigure.cache.CacheType;
import org.springframework.boot.test.autoconfigure.core.AutoConfigureCache;
import org.springframework.cloud.autoconfigure.RefreshAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import datawave.core.common.connection.AccumuloClientPool;
import datawave.marking.MarkingFunctions;
import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.QueryMetricFactory;
import datawave.microservice.querymetric.factory.QueryMetricQueryLogicFactory;
import datawave.microservice.querymetric.handler.QueryMetricCombiner;
import datawave.microservice.querymetric.handler.ShardTableQueryMetricHandler;
import datawave.microservice.security.util.DnUtils;
import datawave.query.language.parser.jexl.LuceneToJexlQueryParser;

@ImportAutoConfiguration({RefreshAutoConfiguration.class})
@AutoConfigureCache(cacheProvider = CacheType.HAZELCAST)
@ComponentScan(basePackages = "datawave.microservice")
@Profile("AlternateQueryMetricTest")
@Configuration
public class AlternateQueryMetricConfiguration {
    
    @Bean
    QueryMetricFactory metricFactory() {
        return new QueryMetricFactory() {
            @Override
            public BaseQueryMetric createMetric() {
                return createMetric(true);
            }
            
            @Override
            public BaseQueryMetric createMetric(boolean populateVersionMap) {
                BaseQueryMetric queryMetric = new AlternateQueryMetric();
                if (populateVersionMap) {
                    queryMetric.populateVersionMap();
                }
                return queryMetric;
            }
        };
    }
    
    @Bean
    public ShardTableQueryMetricHandler shardTableQueryMetricHandler(QueryMetricHandlerProperties queryMetricHandlerProperties,
                    @Qualifier("warehouse") AccumuloClientPool accumuloClientPool, QueryMetricQueryLogicFactory logicFactory, QueryMetricFactory metricFactory,
                    MarkingFunctions markingFunctions, QueryMetricCombiner queryMetricCombiner, LuceneToJexlQueryParser luceneToJexlQueryParser,
                    DnUtils dnUtils) {
        return new AlternateShardTableQueryMetricHandler(queryMetricHandlerProperties, accumuloClientPool, logicFactory, metricFactory, markingFunctions,
                        queryMetricCombiner, luceneToJexlQueryParser, dnUtils);
    }
}
