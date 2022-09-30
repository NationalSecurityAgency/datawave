package datawave.webservice.query.logic;

import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.DatawaveUser.UserType;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.util.DnUtils.NpeUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.easymock.EasyMock;
import org.easymock.EasyMockExtension;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.test.util.ReflectionTestUtils;

import javax.ejb.EJBContext;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.easymock.EasyMock.expect;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(EasyMockExtension.class)
public class ConfiguredQueryLogicFactoryBeanTest extends EasyMockSupport {
    
    QueryLogicFactoryImpl bean = new QueryLogicFactoryImpl();

    @Mock
    QueryLogicFactoryConfiguration altFactoryConfig;

    @Mock
    DatawavePrincipal altPrincipal;
    
    @Mock
    ApplicationContext applicationContext;

    BaseQueryLogic<?> logic;
    
    QueryLogicFactoryConfiguration factoryConfig;

    @Mock
    EJBContext ctx;
    DatawavePrincipal principal;
    
    @BeforeEach
    public void setup() throws IllegalArgumentException, IllegalAccessException {
        System.setProperty(NpeUtils.NPE_OU_PROPERTY, "iamnotaperson");
        System.setProperty("dw.metadatahelper.all.auths", "A,B,C,D");
        Logger.getLogger(ClassPathXmlApplicationContext.class).setLevel(Level.OFF);
        Logger.getLogger(XmlBeanDefinitionReader.class).setLevel(Level.OFF);
        Logger.getLogger(DefaultListableBeanFactory.class).setLevel(Level.OFF);
        ClassPathXmlApplicationContext queryFactory = new ClassPathXmlApplicationContext();
        queryFactory.setConfigLocation("TestConfiguredQueryLogicFactory.xml");
        queryFactory.refresh();
        factoryConfig = queryFactory.getBean(QueryLogicFactoryConfiguration.class.getSimpleName(), QueryLogicFactoryConfiguration.class);
        
        ReflectionTestUtils.setField(bean, "queryLogicFactoryConfiguration", factoryConfig);
        ReflectionTestUtils.setField(bean, "applicationContext", queryFactory);

        logic = createMockBuilder(BaseQueryLogic.class).addMockedMethods("setLogicName", "getMaxPageSize", "getPageByteTrigger").createMock();
        DatawaveUser user = new DatawaveUser(SubjectIssuerDNPair.of("CN=Poe Edgar Allan eapoe, OU=acme", "<CN=ca, OU=acme>"), UserType.USER, null, null, null,
                        0L);
        principal = new DatawavePrincipal(Collections.singletonList(user));
    }

    @Test
    public void testGetQueryLogicWrongName() throws IllegalArgumentException, CloneNotSupportedException {
        EasyMock.expect(ctx.getCallerPrincipal()).andReturn(principal);
        assertThrows(IllegalArgumentException.class, () -> bean.getQueryLogic("TestQuery2", principal));
    }

    @Test
    public void testGetQueryLogic() throws IllegalArgumentException, CloneNotSupportedException {
        EasyMock.expect(ctx.getCallerPrincipal()).andReturn(principal);
        TestQueryLogic<?> logic = (TestQueryLogic<?>) bean.getQueryLogic("TestQuery", principal);
        assertEquals("MyMetadataTable", logic.getTableName());
        assertEquals(123456, logic.getMaxResults());
        assertEquals(987654, logic.getMaxWork());
    }

    @Test
    public void testGetQueryLogic_HasRequiredRoles() throws Exception {
        // Set the query name
        String queryName = "TestQuery";
        String mappedQueryName = "TestQuery2";
        Collection<String> roles = Arrays.asList("Monkey King", "Monkey Queen");

        // Set expectations
        QueryLogicFactoryConfiguration qlfc = new QueryLogicFactoryConfiguration();
        qlfc.setMaxPageSize(25);
        qlfc.setPageByteTrigger(1024L);
        this.logic.setPrincipal(altPrincipal);
        this.logic.setLogicName(queryName);
        expect(this.logic.getMaxPageSize()).andReturn(25);
        expect(this.logic.getPageByteTrigger()).andReturn(1024L);
        expect(this.applicationContext.getBean(mappedQueryName)).andReturn(this.logic);

        // Run the test
        replayAll();
        QueryLogicFactoryImpl subject = new QueryLogicFactoryImpl();
        ReflectionTestUtils.setField(subject, "queryLogicFactoryConfiguration", factoryConfig);
        ReflectionTestUtils.setField(subject, "applicationContext", this.applicationContext);
        QueryLogic<?> result1 = subject.getQueryLogic(queryName, this.altPrincipal);
        verifyAll();

        // Verify results
        assertSame(this.logic, result1, "Query logic should not return null");
    }

    @Test
    public void testGetQueryLogic_propertyOverride() throws Exception {
        // Set the query name
        String queryName = "TestQuery";
        Collection<String> roles = Arrays.asList("Monkey King", "Monkey Queen");
        
        // Set expectations
        QueryLogicFactoryConfiguration qlfc = new QueryLogicFactoryConfiguration();
        qlfc.setMaxPageSize(25);
        qlfc.setPageByteTrigger(1024L);
        
        Map<String,Collection<String>> rolesMap = new HashMap<>();
        rolesMap.put(queryName, roles);
        
        this.logic.setPrincipal(altPrincipal);
        this.logic.setLogicName(queryName);
        expect(this.logic.getMaxPageSize()).andReturn(0);
        expect(this.logic.getPageByteTrigger()).andReturn(0L);
        this.logic.setMaxPageSize(25);
        this.logic.setPageByteTrigger(1024L);
        expect(this.applicationContext.getBean(queryName)).andReturn(this.logic);
        
        // Run the test
        replayAll();
        QueryLogicFactoryImpl subject = new QueryLogicFactoryImpl();
        ReflectionTestUtils.setField(subject, "queryLogicFactoryConfiguration", qlfc);
        ReflectionTestUtils.setField(subject, "applicationContext", this.applicationContext);
        QueryLogic<?> result1 = subject.getQueryLogic(queryName, this.altPrincipal);
        verifyAll();
        
        // Verify results
        assertSame(this.logic, result1, "Query logic should not return null");
    }

    @Test
    public void testQueryLogicList() throws Exception {
        // Run the test
        replayAll();
        List<QueryLogic<?>> result1 = bean.getQueryLogicList();
        verifyAll();

        // Verify results
        assertNotNull(result1, "Query logic list should not return null");
        assertEquals(1, result1.size(), "Query logic list should return with 1 item");
        QueryLogic logic = result1.iterator().next();
        assertEquals("TestQuery", logic.getLogicName());
        assertEquals(123456, logic.getMaxResults());
        assertEquals(987654, logic.getMaxWork());
    }
    
}
