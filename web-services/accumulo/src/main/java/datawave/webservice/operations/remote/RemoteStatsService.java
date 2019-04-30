package datawave.webservice.operations.remote;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationIntrospector;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationModule;
import datawave.configuration.RefreshableScope;
import datawave.webservice.response.StatsResponse;

import javax.annotation.PostConstruct;
import javax.annotation.Priority;
import javax.enterprise.inject.Alternative;
import javax.interceptor.Interceptor;

@RefreshableScope
@Alternative
@Priority(Interceptor.Priority.APPLICATION)
public class RemoteStatsService extends RemoteAccumuloService {
    
    private static final String STATS_SUFFIX = "stats";
    
    private ObjectMapper statsMapper;
    private ObjectReader statsReader;
    
    @Override
    @PostConstruct
    public void init() {
        super.init();
        statsMapper = new ObjectMapper();
        statsMapper.registerModule(new JaxbAnnotationModule());
        statsMapper.setAnnotationIntrospector(new JaxbAnnotationIntrospector(TypeFactory.defaultInstance()));
        statsReader = statsMapper.readerFor(StatsResponse.class);
    }
    
    @Timed(name = "dw.remoteAccumuloService.stats", absolute = true)
    public StatsResponse getStats() {
        return execGet(STATS_SUFFIX, statsReader);
    }
}
