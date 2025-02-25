package datawave.microservice.querymetric.factory;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import datawave.marking.MarkingFunctions;
import datawave.microservice.querymetric.config.QueryMetricHandlerProperties;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.util.DateIndexHelperFactory;
import datawave.query.util.MetadataHelperFactory;
import datawave.webservice.common.audit.Auditor;
import datawave.webservice.query.result.event.ResponseObjectFactory;

@Component
public class QueryMetricQueryLogicFactory implements FactoryBean<ShardQueryLogic> {
    
    private MarkingFunctions markingFunctions;
    private ResponseObjectFactory responseObjectFactory;
    private QueryMetricHandlerProperties queryMetricHandlerProperties;
    private DateIndexHelperFactory dateIndexHelperFactory;
    private MetadataHelperFactory metadataHelperFactory;
    
    @Autowired
    public QueryMetricQueryLogicFactory(MarkingFunctions markingFunctions, ResponseObjectFactory responseObjectFactory,
                    DateIndexHelperFactory dateIndexHelperFactory, MetadataHelperFactory metadataHelperFactory,
                    QueryMetricHandlerProperties queryMetricHandlerProperties) {
        this.markingFunctions = markingFunctions;
        this.responseObjectFactory = responseObjectFactory;
        this.dateIndexHelperFactory = dateIndexHelperFactory;
        this.metadataHelperFactory = metadataHelperFactory;
        this.queryMetricHandlerProperties = queryMetricHandlerProperties;
    }
    
    @Override
    public ShardQueryLogic getObject() throws Exception {
        
        ShardQueryLogic logic = new ShardQueryLogic();
        logic.setMarkingFunctions(markingFunctions);
        logic.setTableName(this.queryMetricHandlerProperties.getShardTableName());
        logic.setIndexTableName(this.queryMetricHandlerProperties.getIndexTableName());
        logic.setReverseIndexTableName(this.queryMetricHandlerProperties.getReverseIndexTableName());
        logic.setMetadataTableName(this.queryMetricHandlerProperties.getMetadataTableName());
        logic.setModelTableName(this.queryMetricHandlerProperties.getMetadataTableName());
        logic.setAuditType(Auditor.AuditType.NONE);
        logic.setModelName("NONE");
        logic.setMetadataHelperFactory(this.metadataHelperFactory);
        logic.setDateIndexHelperFactory(this.dateIndexHelperFactory);
        logic.setResponseObjectFactory(this.responseObjectFactory);
        logic.setAccumuloPassword(this.queryMetricHandlerProperties.getPassword());
        logic.setMaxEvaluationPipelines(1); // use SerialIterator
        return logic;
    }
    
    @Override
    public Class<?> getObjectType() {
        return ShardQueryLogic.class;
    }
}
