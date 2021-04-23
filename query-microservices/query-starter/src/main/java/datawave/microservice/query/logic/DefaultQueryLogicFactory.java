package datawave.microservice.query.logic;

import datawave.microservice.query.config.QueryLogicFactoryProperties;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.exception.UnauthorizedQueryException;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

@Component
@ConditionalOnProperty(name = "query.logic.factory.enabled", havingValue = "true", matchIfMissing = true)
public class DefaultQueryLogicFactory implements QueryLogicFactory, ApplicationContextAware {
    
    /**
     * Configuration for parameters that are for all query logic types
     */
    private QueryLogicFactoryProperties queryLogicFactoryProperties;
    
    private ApplicationContext applicationContext;
    
    public DefaultQueryLogicFactory(QueryLogicFactoryProperties queryLogicFactoryProperties) {
        this.queryLogicFactoryProperties = queryLogicFactoryProperties;
    }
    
    @Override
    public QueryLogic<?> getQueryLogic(String name, Collection<String> userRoles) throws QueryException {
        return getQueryLogic(name, userRoles, true);
    }
    
    @Override
    public QueryLogic<?> getQueryLogic(String name) throws QueryException {
        return getQueryLogic(name, null, false);
    }
    
    private QueryLogic<?> getQueryLogic(String name, Collection<String> userRoles, boolean checkRoles) throws QueryException {
        QueryLogic<?> logic;
        try {
            logic = (QueryLogic<?>) applicationContext.getBean(name);
        } catch (ClassCastException | NoSuchBeanDefinitionException cce) {
            throw new IllegalArgumentException("Logic name '" + name + "' does not exist in the configuration");
        }
        
        if (checkRoles && !logic.canRunQuery(userRoles)) {
            throw new UnauthorizedQueryException(DatawaveErrorCode.MISSING_REQUIRED_ROLES,
                            new IllegalAccessException("User does not have required role(s): " + logic.getRequiredRoles()));
        }
        
        logic.setLogicName(name);
        if (logic.getMaxPageSize() == 0) {
            logic.setMaxPageSize(queryLogicFactoryProperties.getMaxPageSize());
        }
        if (logic.getPageByteTrigger() == 0) {
            logic.setPageByteTrigger(queryLogicFactoryProperties.getPageByteTrigger());
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
    
    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}
