package datawave.webservice.query.logic;

import java.security.Principal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import datawave.configuration.spring.SpringBean;
import datawave.webservice.common.exception.UnauthorizedException;
import datawave.webservice.result.VoidResponse;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.context.ApplicationContext;

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
    public QueryLogic<?> getQueryLogic(String name, Principal principal) throws IllegalArgumentException, CloneNotSupportedException {
        
        QueryLogic<?> logic;
        try {
            logic = (QueryLogic<?>) applicationContext.getBean(name);
            logic.setPrincipal(principal);
        } catch (ClassCastException | NoSuchBeanDefinitionException cce) {
            throw new IllegalArgumentException("Logic name '" + name + "' does not exist in the configuration");
        }
        
        if (!logic.canRunQuery(principal)) {
            throw new UnauthorizedException(new IllegalAccessException("User does not have required role(s): " + logic.getRoleManager().getRequiredRoles()),
                            new VoidResponse());
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
