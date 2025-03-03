package datawave.microservice.query.logic;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import datawave.core.query.logic.QueryLogic;
import datawave.core.query.logic.QueryLogicFactory;
import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.microservice.query.logic.config.QueryLogicFactoryProperties;
import datawave.security.authorization.ProxiedUserDetails;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.exception.UnauthorizedQueryException;

@Component
@ConditionalOnProperty(name = "datawave.query.logic.factory.enabled", havingValue = "true", matchIfMissing = true)
public class DefaultQueryLogicFactory implements QueryLogicFactory, ApplicationContextAware {
    private final Logger log = LoggerFactory.getLogger(getClass());
    
    /**
     * Configuration for parameters that are for all query logic types
     */
    private final QueryLogicFactoryProperties queryLogicFactoryProperties;
    
    private ApplicationContext applicationContext;
    
    private final Supplier<DatawaveUserDetails> serverUserDetailsSupplier;
    
    public DefaultQueryLogicFactory(QueryLogicFactoryProperties queryLogicFactoryProperties,
                    @Autowired(required = false) @Qualifier("serverUserDetailsSupplier") Supplier<DatawaveUserDetails> serverUserDetailsSupplier) {
        this.queryLogicFactoryProperties = queryLogicFactoryProperties;
        this.serverUserDetailsSupplier = serverUserDetailsSupplier;
    }
    
    @Override
    public QueryLogic<?> getQueryLogic(String name, ProxiedUserDetails currentUser) throws QueryException {
        return getQueryLogic(name, currentUser, true);
    }
    
    @Override
    public QueryLogic<?> getQueryLogic(String name) throws QueryException {
        return getQueryLogic(name, null, false);
    }
    
    private QueryLogic<?> getQueryLogic(String name, ProxiedUserDetails currentUser, boolean checkRoles) throws QueryException {
        String queryLogic = name;
        
        if (!queryLogicFactoryProperties.getQueryLogicsByName().isEmpty()) {
            queryLogic = queryLogicFactoryProperties.getQueryLogicsByName().get(name);
        }
        
        if (queryLogic == null) {
            throw new IllegalArgumentException("Logic name '" + name + "' is not configured for this system");
        }
        
        QueryLogic<?> logic;
        try {
            logic = (QueryLogic<?>) applicationContext.getBean(name);
        } catch (ClassCastException | NoSuchBeanDefinitionException cce) {
            if (queryLogic.equals(name)) {
                throw new IllegalArgumentException("Logic name '" + name + "' does not exist in the configuration");
            } else {
                throw new IllegalArgumentException("Logic name '" + name + "' which maps to '" + queryLogic + "' does not exist in the configuration");
            }
        }
        
        Set<String> userRoles = new HashSet<>();
        if (currentUser != null) {
            userRoles.addAll(currentUser.getPrimaryUser().getRoles());
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
        
        // update server user details
        logic.setCurrentUser(currentUser);
        
        if (serverUserDetailsSupplier != null) {
            logic.setServerUser(serverUserDetailsSupplier.get());
        }
        
        return logic;
    }
    
    @Override
    public List<QueryLogic<?>> getQueryLogicList() {
        Map<String,QueryLogic> logicMap = applicationContext.getBeansOfType(QueryLogic.class);
        
        if (!queryLogicFactoryProperties.getQueryLogicsByName().isEmpty()) {
            Map<String,QueryLogic> renamedLogicMap = new LinkedHashMap<>();
            for (Map.Entry<String,String> entry : queryLogicFactoryProperties.getQueryLogicsByName().entrySet()) {
                if (logicMap.containsKey(entry.getValue())) {
                    renamedLogicMap.put(entry.getKey(), logicMap.get(entry.getValue()));
                }
            }
            logicMap = renamedLogicMap;
        }
        
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
