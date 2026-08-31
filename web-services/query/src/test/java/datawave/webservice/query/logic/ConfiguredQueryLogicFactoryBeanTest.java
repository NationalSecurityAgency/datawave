package datawave.webservice.query.logic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.core.query.logic.BaseQueryLogic;
import datawave.core.query.logic.QueryLogic;
import datawave.core.query.logic.QueryLogicFactory;
import datawave.microservice.query.logic.config.QueryLogicFactoryProperties;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.DatawaveUser.UserType;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.webservice.common.exception.UnauthorizedException;
import datawave.webservice.query.exception.QueryException;

/**
 * This test exercises several features of the {@link QueryLogicFactory}, notably the ability to remap logics based on name, and the ability to restrict access
 * to a query logic via user roles.
 */
@ExtendWith(SpringExtension.class)
@ComponentScan(basePackages = "datawave.webservice.query.logic")
@ContextConfiguration(locations = "classpath:TestConfiguredQueryLogicFactory.xml")
public class ConfiguredQueryLogicFactoryBeanTest {

    @Autowired
    @Qualifier("UserQueryLogic")
    private BaseQueryLogic<?> userQueryLogic;

    @Autowired
    @Qualifier("AdminQueryLogic")
    private BaseQueryLogic<?> adminQueryLogic;

    private final QueryLogicFactory queryLogicFactory = new QueryLogicFactoryImpl();

    private static final String USER_LOGIC_NAME = "UserQueryLogic";
    private static final String ADMIN_LOGIC_NAME = "AdminQueryLogic";
    private static final String ADMINISTRATOR_LOGIC_NAME = "AdministratorQueryLogic";

    private final DatawavePrincipal USER_PRINCIPLE = getUserPrinciple();
    private final DatawavePrincipal ADMIN_PRINCIPLE = getAdminPrinciple();

    private final String USER_ROLE = "user";
    private final String ADMIN_ROLE = "admin";

    private Map<String,String> alternateLogicMap;

    @BeforeEach
    @SuppressWarnings("unchecked")
    public void beforeEach(ApplicationContext context) throws IllegalArgumentException, IllegalAccessException {
        assertNotNull(context);

        ClassPathXmlApplicationContext queryFactory = new ClassPathXmlApplicationContext();
        queryFactory.setConfigLocation("TestConfiguredQueryLogicFactory.xml");
        queryFactory.refresh();
        QueryLogicFactoryProperties factoryProperties = queryFactory.getBean("queryLogicFactoryProperties", QueryLogicFactoryProperties.class);

        ReflectionTestUtils.setField(queryLogicFactory, "queryLogicFactoryProperties", factoryProperties);
        ReflectionTestUtils.setField(queryLogicFactory, "applicationContext", context);

        alternateLogicMap = (Map<String,String>) queryFactory.getBean("ExpandedLogicMap");
    }

    @Test
    public void testGetQueryLogicThatDoesNotExist() {
        assertThrows(IllegalArgumentException.class, () -> queryLogicFactory.getQueryLogic("SuperUserQueryLogic"));
    }

    // fun fact, this method always fails in the default implementation
    @Test
    public void testGetQueryLogicThatExistsButNoProxiedUser() {
        assertThrows(NullPointerException.class, () -> queryLogicFactory.getQueryLogic(USER_LOGIC_NAME));
    }

    @Test
    public void testUserGetsUserQueryLogic() throws QueryException, CloneNotSupportedException {
        QueryLogic<?> logic = queryLogicFactory.getQueryLogic(USER_LOGIC_NAME, USER_PRINCIPLE);
        assertEquals(userQueryLogic, logic);
    }

    @Test
    public void testUserCannotGetAdminQueryLogic() {
        assertThrows(UnauthorizedException.class, () -> queryLogicFactory.getQueryLogic(ADMIN_LOGIC_NAME, USER_PRINCIPLE));
    }

    @Test
    public void testAdminGetsAdminQueryLogic() throws QueryException, CloneNotSupportedException {
        QueryLogic<?> logic = queryLogicFactory.getQueryLogic(ADMIN_LOGIC_NAME, ADMIN_PRINCIPLE);
        assertEquals(adminQueryLogic, logic);
    }

    @Test
    public void testAdminGetsUserQueryLogic() throws QueryException, CloneNotSupportedException {
        QueryLogic<?> logic = queryLogicFactory.getQueryLogic(USER_LOGIC_NAME, ADMIN_PRINCIPLE);
        assertEquals(userQueryLogic, logic);
    }

    @Test
    public void testListQueryLogic() {
        List<QueryLogic<?>> queryLogics = queryLogicFactory.getQueryLogicList();
        assertEquals(2, queryLogics.size());
        queryLogics.sort(Comparator.comparing(QueryLogic::getLogicName));
        assertEquals(adminQueryLogic, queryLogics.get(0));
        assertEquals(userQueryLogic, queryLogics.get(1));
    }

    @Test
    public void testUserQueryLogicRoles() throws QueryException, CloneNotSupportedException {
        QueryLogic<?> logic = queryLogicFactory.getQueryLogic(USER_LOGIC_NAME, USER_PRINCIPLE);
        assertEquals(Set.of(USER_ROLE), logic.getRequiredRoles());
    }

    @Test
    public void testAdminQueryLogicRoles() throws QueryException, CloneNotSupportedException {
        QueryLogic<?> logic = queryLogicFactory.getQueryLogic(ADMIN_LOGIC_NAME, ADMIN_PRINCIPLE);
        assertEquals(Set.of(ADMIN_ROLE), logic.getRequiredRoles());
    }

    @Test
    public void testMappingLogicName() throws QueryException, CloneNotSupportedException {
        // for this test someone with the ADMINISTRATOR role should be redirected to the ADMIN logic
        QueryLogicFactoryProperties properties = (QueryLogicFactoryProperties) ReflectionTestUtils.getField(queryLogicFactory, "queryLogicFactoryProperties");
        assertNotNull(properties);
        properties.setQueryLogicsByName(alternateLogicMap);

        QueryLogic<?> logic = queryLogicFactory.getQueryLogic(ADMINISTRATOR_LOGIC_NAME, ADMIN_PRINCIPLE);
        assertEquals(adminQueryLogic, logic);
    }

    private DatawavePrincipal getUserPrinciple() {
        Set<String> roles = Set.of(USER_ROLE);
        return getPrinciple("CN=Poe Edgar Allan eapoe, OU=acme", "<CN=ca, OU=acme>", roles);
    }

    private DatawavePrincipal getAdminPrinciple() {
        Set<String> roles = Set.of(ADMIN_ROLE, USER_ROLE);
        return getPrinciple("CN=Last First Middle fmlast, OU=acme", "<CN=ca, OU=acme>", roles);
    }

    private DatawavePrincipal getPrinciple(String subjectDN, String issuerDN, Set<String> roles) {
        SubjectIssuerDNPair dnPair = SubjectIssuerDNPair.of(subjectDN, issuerDN);
        Set<String> auths = Collections.emptySet();
        Multimap<String,String> roleToAuthMapping = HashMultimap.create();
        DatawaveUser user = new DatawaveUser(dnPair, UserType.USER, auths, roles, roleToAuthMapping, 0L);
        return new DatawavePrincipal(Set.of(user));
    }

}
