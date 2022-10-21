package datawave.webservice.query.logic;

import datawave.configuration.spring.SpringBean;
import datawave.core.query.logic.BaseQueryLogic;
import datawave.core.query.logic.QueryLogic;
import datawave.core.query.logic.QueryLogicFactory;
import datawave.microservice.authorization.user.ProxiedUserDetails;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.system.ServerPrincipal;
import datawave.webservice.common.exception.UnauthorizedException;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.result.VoidResponse;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.context.ApplicationContext;

import javax.inject.Inject;
import java.security.Principal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    
    @Inject
    @ServerPrincipal
    private DatawavePrincipal serverPrincipal;
    
    @Override
    public QueryLogic<?> getQueryLogic(String name, Principal principal) throws IllegalArgumentException, CloneNotSupportedException {
        return getQueryLogic(name, principal, true);
    }
    
    @Override
    public QueryLogic<?> getQueryLogic(String name, ProxiedUserDetails currentUser)
                    throws QueryException, IllegalArgumentException, CloneNotSupportedException {
        throw new UnsupportedOperationException("Using proxied user details to create a query logic is not supported for wildfly deployments");
    }
    
    @Override
    public QueryLogic<?> getQueryLogic(String name) throws IllegalArgumentException, CloneNotSupportedException {
        return getQueryLogic(name, null, false);
    }
    
    private QueryLogic<?> getQueryLogic(String name, Principal principal, boolean checkRoles) throws IllegalArgumentException, CloneNotSupportedException {
        QueryLogic<?> logic;
        try {
            logic = (QueryLogic<?>) applicationContext.getBean(name);
        } catch (ClassCastException | NoSuchBeanDefinitionException cce) {
            throw new IllegalArgumentException("Logic name '" + name + "' does not exist in the configuration");
        }
        
        Set<String> userRoles = new HashSet<>();
        if (principal instanceof DatawavePrincipal) {
            userRoles.addAll(((DatawavePrincipal) principal).getPrimaryUser().getRoles());
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
        
        if (logic instanceof BaseQueryLogic) {
            ((BaseQueryLogic<?>) logic).setPrincipal(principal);
            ((BaseQueryLogic<?>) logic).setServerPrincipal(serverPrincipal);
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
