package datawave.webservice.query.limit;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Method;

import javax.annotation.security.DeclareRoles;
import javax.annotation.security.RolesAllowed;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import datawave.webservice.query.runner.QueryExecutorBean;

class QueryLimiterSecurityTest {

    @ParameterizedTest
    @ValueSource(strings = {"adminClose", "adminCancel"})
    void allowsHeartbeatCleanupForAdministratorEndpoints(String endpoint) throws NoSuchMethodException {
        String[] callerRoles = QueryExecutorBean.class.getMethod(endpoint, String.class).getAnnotation(RolesAllowed.class).value();
        Method cleanup = QueryLimiterImpl.class.getMethod("markInactive", String.class);
        RolesAllowed cleanupRoles = cleanup.getAnnotation(RolesAllowed.class);
        if (cleanupRoles == null) {
            cleanupRoles = QueryLimiterImpl.class.getAnnotation(RolesAllowed.class);
        }

        assertThat(cleanupRoles.value()).as("limiter permissions for callers of %s", endpoint).contains(callerRoles);
        assertThat(QueryLimiterImpl.class.getAnnotation(DeclareRoles.class).value()).as("declared roles for callers of %s", endpoint).contains(callerRoles);
    }
}
