package datawave.webservice.query.logic;

import datawave.configuration.spring.SpringBean;
import datawave.microservice.query.logic.QueryLogic;
import datawave.microservice.query.logic.QueryLogicFactory;
import datawave.webservice.common.exception.UnauthorizedException;
import datawave.webservice.result.VoidResponse;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.context.ApplicationContext;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

// TODO: JWO: Remove this once we finally move away from the WildFly Webservice
@Deprecated
public class QueryLogicFactoryImpl implements QueryLogicFactory {
    
    /**
     * Configuration for parameters that are for all query logic types
     */
    @Inject
    @SpringBean(refreshable = true)
    private QueryLogicFactoryConfiguration queryLogicFactoryConfiguration;
    
    @Inject
    private ApplicationContext applicationContext;
    
    @Override
    public QueryLogic<?> getQueryLogic(String name, Collection<String> userRoles) throws IllegalArgumentException, CloneNotSupportedException {
        return getQueryLogic(name, userRoles, true);
    }
    
    @Override
    public QueryLogic<?> getQueryLogic(String name) throws IllegalArgumentException, CloneNotSupportedException {
        return getQueryLogic(name, null, false);
    }
    
    private QueryLogic<?> getQueryLogic(String name, Collection<String> userRoles, boolean checkRoles) throws IllegalArgumentException,
                    CloneNotSupportedException {
        QueryLogic<?> logic;
        try {
            logic = (QueryLogic<?>) applicationContext.getBean(name);
        } catch (ClassCastException | NoSuchBeanDefinitionException cce) {
            throw new IllegalArgumentException("Logic name '" + name + "' does not exist in the configuration");
        }
        
        if (checkRoles && !logic.canRunQuery(userRoles)) {
            throw new UnauthorizedException(new IllegalAccessException("User does not have required role(s): " + logic.getRequiredRoles()), new VoidResponse());
        }
        
        logic.setLogicName(name);
        if (logic.getMaxPageSize() == 0) {
            logic.setMaxPageSize(queryLogicFactoryConfiguration.getMaxPageSize());
        }
        if (logic.getPageByteTrigger() == 0) {
            logic.setPageByteTrigger(queryLogicFactoryConfiguration.getPageByteTrigger());
        }
        return logic;
    }
    
    @Override
    public List<QueryLogic<?>> getQueryLogicList() {
        Map<String,QueryLogic> logicMap = applicationContext.getBeansOfType(QueryLogic.class);
        
        List<QueryLogic<?>> logicList = new ArrayList<>();
        
        for (Map.Entry<String,QueryLogic> entry : logicMap.entrySet()) {
            QueryLogic<?> logic = entry.getValue();
            logic.setLogicName(entry.getKey());
            logicList.add(logic);
        }
        return logicList;
        
    }
}
