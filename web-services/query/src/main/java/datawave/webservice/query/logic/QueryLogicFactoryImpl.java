package datawave.webservice.query.logic;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.context.ApplicationContext;

import datawave.configuration.spring.SpringBean;
import datawave.core.query.logic.QueryLogic;
import datawave.core.query.logic.QueryLogicFactory;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.ProxiedUserDetails;
import datawave.security.system.ServerPrincipal;
import datawave.webservice.common.exception.UnauthorizedException;
import datawave.webservice.result.VoidResponse;

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
    public QueryLogic<?> getQueryLogic(String name, ProxiedUserDetails currentUser) throws IllegalArgumentException, CloneNotSupportedException {
        return getQueryLogic(name, currentUser, true);
    }

    @Override
    public QueryLogic<?> getQueryLogic(String name) throws IllegalArgumentException, CloneNotSupportedException {
        return getQueryLogic(name, null, false);
    }

    public QueryLogic<?> getQueryLogic(String queryLogic, ProxiedUserDetails currentUser, boolean checkRoles)
                    throws IllegalArgumentException, CloneNotSupportedException {
        String beanName = queryLogic;
        if (queryLogicFactoryConfiguration.hasLogicMap()) {
            beanName = queryLogicFactoryConfiguration.getLogicMap().get(queryLogic);
        }
        if (beanName == null) {
            throw new IllegalArgumentException("Logic name '" + queryLogic + "' is not configured for this system");
        }

        QueryLogic<?> logic;
        try {
            logic = (QueryLogic<?>) applicationContext.getBean(beanName);
            logic.setCurrentUser(currentUser);
        } catch (ClassCastException | NoSuchBeanDefinitionException cce) {
            if (beanName.equals(queryLogic)) {
                throw new IllegalArgumentException("Logic name '" + queryLogic + "' does not exist in the configuration");
            } else {
                throw new IllegalArgumentException("Logic name '" + queryLogic + "' which maps to '" + beanName + "' does not exist in the configuration");
            }
        }

        Set<String> userRoles = new HashSet<>(currentUser.getPrimaryUser().getRoles());
        if (checkRoles && !logic.canRunQuery(userRoles)) {
            throw new UnauthorizedException(new IllegalAccessException("User does not have required role(s): " + logic.getRequiredRoles()), new VoidResponse());
        }

        logic.setLogicName(queryLogic);
        if (logic.getMaxPageSize() == 0) {
            logic.setMaxPageSize(queryLogicFactoryConfiguration.getMaxPageSize());
        }
        if (logic.getPageByteTrigger() == 0) {
            logic.setPageByteTrigger(queryLogicFactoryConfiguration.getPageByteTrigger());
        }

        logic.setCurrentUser(currentUser);
        logic.setServerUser(serverPrincipal);

        return logic;
    }

    @Override
    public List<QueryLogic<?>> getQueryLogicList() {
        Map<String,QueryLogic> logicMap = applicationContext.getBeansOfType(QueryLogic.class);
        if (queryLogicFactoryConfiguration.hasLogicMap()) {
            Map<String,QueryLogic> renamedLogicMap = new HashMap<>();
            for (Map.Entry<String,String> entry : queryLogicFactoryConfiguration.getLogicMap().entrySet()) {
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
}
