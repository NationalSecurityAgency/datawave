package datawave.microservice.querymetric.factory;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

import datawave.query.util.MetadataHelperFactory;
import datawave.query.util.TypeMetadataHelper;

@Component
@Primary
public class QueryMetricMetadataHelperFactory extends MetadataHelperFactory {
    
    @Autowired
    public QueryMetricMetadataHelperFactory(BeanFactory beanFactory, @Qualifier("queryMetrics") TypeMetadataHelper.Factory typeMetadataHelperFactory) {
        super(beanFactory, typeMetadataHelperFactory);
    }
}
