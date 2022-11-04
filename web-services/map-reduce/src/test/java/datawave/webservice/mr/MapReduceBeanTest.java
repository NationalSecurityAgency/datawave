package datawave.webservice.mr;

import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.DatawaveUser.UserType;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.util.DnUtils.NpeUtils;
import datawave.webservice.common.connection.AccumuloConnectionFactory;
import datawave.webservice.common.connection.config.ConnectionPoolsConfiguration;
import datawave.webservice.common.exception.BadRequestException;
import datawave.webservice.common.exception.DatawaveWebApplicationException;
import datawave.webservice.common.exception.NoResultsException;
import datawave.webservice.common.exception.UnauthorizedException;
import datawave.webservice.mr.configuration.MapReduceConfiguration;
import datawave.webservice.mr.configuration.MapReduceJobConfiguration;
import datawave.webservice.mr.state.MapReduceStatePersisterBean;
import datawave.webservice.query.cache.QueryCache;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.factory.Persister;
import datawave.webservice.query.logic.QueryLogicFactory;
import datawave.webservice.results.mr.MapReduceJobDescription;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.easymock.EasyMockExtension;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.easymock.TestSubject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.test.util.ReflectionTestUtils;

import javax.ejb.EJBContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.easymock.EasyMock.expect;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(EasyMockExtension.class)
public class MapReduceBeanTest extends EasyMockSupport {
    @TestSubject
    private TestMapReduceBean bean = new TestMapReduceBean();
    
    @Mock
    private EJBContext ctx;
    @Mock
    private Persister queryPersister;
    @Mock
    private QueryCache cache;
    @Mock
    private AccumuloConnectionFactory connectionFactory;
    @Mock
    private QueryLogicFactory queryLogicFactory;
    @Mock
    private MapReduceStatePersisterBean mapRedbeanuceState;
    @Mock
    private ConnectionPoolsConfiguration connectionPoolsConfiguration;
    
    private DatawavePrincipal principal;
    private ClassPathXmlApplicationContext applicationContext;
    
    private static final String userDN = "CN=Guy Some Other soguy, OU=acme";
    private static final String[] auths = new String[] {"AUTHS"};
    
    @BeforeEach
    public void setup() throws Exception {
        System.setProperty(NpeUtils.NPE_OU_PROPERTY, "iamnotaperson");
        System.setProperty("dw.metadatahelper.all.auths", "A,B,C,D");
        DatawaveUser user = new DatawaveUser(SubjectIssuerDNPair.of(userDN, "CN=ca, OU=acme"), UserType.USER, Arrays.asList(auths),
                        Collections.singleton("AuthorizedUser"), null, 0L);
        principal = new DatawavePrincipal(Collections.singletonList(user));
        
        applicationContext = new ClassPathXmlApplicationContext("classpath:*datawave/mapreduce/MapReduceJobs.xml");
        ReflectionTestUtils.setField(bean, "mapReduceConfiguration", applicationContext.getBean(MapReduceConfiguration.class));
    }
    
    @Test
    public void testListConfiguredMapReduceJobs() throws Exception {
        
        replayAll();
        List<MapReduceJobDescription> configs = bean.listConfiguredMapReduceJobs("none");
        verifyAll();
        
        assertEquals(1, configs.size());
        assertEquals("TestJob", configs.get(0).getName());
        assertEquals("MapReduce job that runs a query and either puts the results into a table or files in HDFS", configs.get(0).getDescription());
        List<String> required = new ArrayList<>();
        required.add("queryId");
        required.add("format");
        assertEquals(required, configs.get(0).getRequiredRuntimeParameters());
        List<String> optional = new ArrayList<>();
        optional.add("outputTableName");
        assertEquals(optional, configs.get(0).getOptionalRuntimeParameters());
        
    }
    
    @Test
    public void testSubmitInvalidJobName() throws Exception {
        expect(ctx.getCallerPrincipal()).andReturn(principal);
        replayAll();
        
        assertThrows(BadRequestException.class, () -> bean.submit("BadJobName", null));
        verifyAll();
    }
    
    @Test
    public void testSubmitWithNullRequiredParameters() throws Exception {
        expect(ctx.getCallerPrincipal()).andReturn(principal);
        replayAll();
        
        assertThrows(DatawaveWebApplicationException.class, () -> bean.submit("TestJob", null));
        verifyAll();
    }
    
    @Test
    public void testSubmitWithMissingRequiredParameters() throws Exception {
        expect(ctx.getCallerPrincipal()).andReturn(principal);
        replayAll();
        
        assertThrows(DatawaveWebApplicationException.class, () -> bean.submit("TestJob", "queryId:1243"));
        verifyAll();
    }
    
    @Test
    public void testInvalidInputFormat() throws Exception {
        Job mockJob = createMock(Job.class);
        bean.setJob(mockJob);
        
        MapReduceConfiguration mrConfig = applicationContext.getBean(MapReduceConfiguration.class);
        mrConfig.getJobConfiguration().clear();
        mrConfig.getJobConfiguration().put("TestJob", new MapReduceJobConfiguration());
        
        // BulkResultsJob uses AccumuloInputFormat, MapReduceJobs.xml in
        // src/test/resources specifies something else
        
        expect(ctx.getCallerPrincipal()).andReturn(principal);
        replayAll();
        
        assertThrows(DatawaveWebApplicationException.class, () -> bean.submit("TestJob", "queryId:1243;format:XML"));
        verifyAll();
    }
    
    @Test
    public void testNoResults() throws Exception {
        Job mockJob = createMock(Job.class);
        bean.setJob(mockJob);
        
        MapReduceJobConfiguration cfg = new MapReduceJobConfiguration() {
            @Override
            public final void initializeConfiguration(String jobId, Job job, Map<String,String> runtimeParameters, DatawavePrincipal serverPrincipal)
                            throws Exception {
                throw new NoResultsException(new QueryException(DatawaveErrorCode.NO_RANGES));
            }
        };
        MapReduceConfiguration mrConfig = applicationContext.getBean(MapReduceConfiguration.class);
        mrConfig.getJobConfiguration().clear();
        mrConfig.getJobConfiguration().put("TestJob", cfg);
        
        // BulkResultsJob uses AccumuloInputFormat, MapReduceJobs.xml in
        // src/test/resources specifies something else
        
        expect(ctx.getCallerPrincipal()).andReturn(principal);
        replayAll();
        
        assertThrows(NoResultsException.class, () -> bean.submit("TestJob", "queryId:1243;format:XML"));
        verifyAll();
    }
    
    @Test
    public void testInvalidUserAuthorization() throws Exception {
        // Create principal that does not have AuthorizedUser role
        DatawaveUser user = new DatawaveUser(SubjectIssuerDNPair.of(userDN, "CN=ca, OU=acme"), UserType.USER, Arrays.asList(auths),
                        Collections.singleton("Administrator"), null, 0L);
        DatawavePrincipal p = new DatawavePrincipal(Collections.singletonList(user));
        
        expect(ctx.getCallerPrincipal()).andReturn(p);
        replayAll();
        
        assertThrows(UnauthorizedException.class, () -> bean.submit("TestJob", "queryId:1243"));
        verifyAll();
    }
    
    private static class TestMapReduceBean extends MapReduceBean {
        private Job job;
        
        @Override
        protected Job createJob(Configuration conf, StringBuilder name) throws IOException {
            if (job == null)
                return Job.getInstance(conf, name.toString());
            return job;
        }
        
        public void setJob(Job job) {
            this.job = job;
        }
    }
}
