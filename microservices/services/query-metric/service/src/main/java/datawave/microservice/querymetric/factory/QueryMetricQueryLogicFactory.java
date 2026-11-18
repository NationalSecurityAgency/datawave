package datawave.microservice.querymetric.factory;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import datawave.marking.MarkingFunctions;
import datawave.microservice.querymetric.config.QueryMetricHandlerProperties;
import datawave.query.planner.DefaultQueryPlanner;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.util.DateIndexHelperFactory;
import datawave.query.util.MetadataHelperFactory;
import datawave.webservice.common.audit.Auditor;
import datawave.webservice.query.result.event.ResponseObjectFactory;

@Component
public class QueryMetricQueryLogicFactory implements FactoryBean<ShardQueryLogic> {

    private ResponseObjectFactory responseObjectFactory;
    private QueryMetricHandlerProperties queryMetricHandlerProperties;
    private DateIndexHelperFactory dateIndexHelperFactory;
    private MetadataHelperFactory metadataHelperFactory;

    @Autowired
    public QueryMetricQueryLogicFactory(ResponseObjectFactory responseObjectFactory, DateIndexHelperFactory dateIndexHelperFactory,
                    MetadataHelperFactory metadataHelperFactory, QueryMetricHandlerProperties queryMetricHandlerProperties) {
        this.responseObjectFactory = responseObjectFactory;
        this.dateIndexHelperFactory = dateIndexHelperFactory;
        this.metadataHelperFactory = metadataHelperFactory;
        this.queryMetricHandlerProperties = queryMetricHandlerProperties;
    }

    @Override
    public ShardQueryLogic getObject() throws Exception {

        ShardQueryLogic logic = new ShardQueryLogic();
        // TODO remove-accumulo-utils: ShardQueryLogic transformers still require MarkingFunctions; remove once query-core no longer takes them
        logic.setMarkingFunctions(new MarkingFunctions.Default());
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
        logic.setQueryPlanner(new DefaultQueryPlanner());
        return logic;
    }

    @Override
    public Class<?> getObjectType() {
        return ShardQueryLogic.class;
    }
}
