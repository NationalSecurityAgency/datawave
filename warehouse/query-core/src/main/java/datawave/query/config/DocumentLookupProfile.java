package datawave.query.config;

import datawave.query.planner.QueryPlanner;
import datawave.query.tables.ShardQueryLogic;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import datawave.webservice.query.logic.BaseQueryLogic;
import datawave.webservice.query.service.ServiceConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import java.util.Map;

public class DocumentLookupProfile implements Profile {
    protected ServiceConfiguration serviceConfiguration = ServiceConfiguration.getDefaultInstance();

    public void setServiceConfiguration(ServiceConfiguration serviceConfiguration){
        this.serviceConfiguration=serviceConfiguration;
    }

    public ServiceConfiguration getServiceConfiguration(){
        return serviceConfiguration;
    }

    @Override
    public void configure(BaseQueryLogic<Map.Entry<Key, Value>> logic) {
       if (logic instanceof ShardQueryLogic){
           ShardQueryLogic sql = ShardQueryLogic.class.cast(logic);
           sql.setServiceConfiguration( serviceConfiguration );
       }
    }

    @Override
    public void configure(QueryPlanner planner) {

    }

    @Override
    public void configure(GenericQueryConfiguration configuration) {

    }
}
