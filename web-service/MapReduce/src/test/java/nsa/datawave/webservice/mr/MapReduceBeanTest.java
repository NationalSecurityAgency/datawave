package nsa.datawave.webservice.mr;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.ejb.EJBContext;
import javax.ws.rs.core.MultivaluedHashMap;

import nsa.datawave.security.authorization.DatawavePrincipal;
import nsa.datawave.webservice.common.connection.AccumuloConnectionFactory;
import nsa.datawave.webservice.common.connection.config.ConnectionPoolsConfiguration;
import nsa.datawave.webservice.common.exception.BadRequestException;
import nsa.datawave.webservice.common.exception.DatawaveWebApplicationException;
import nsa.datawave.webservice.common.exception.NoResultsException;
import nsa.datawave.webservice.common.exception.UnauthorizedException;
import nsa.datawave.webservice.mr.configuration.MapReduceConfiguration;
import nsa.datawave.webservice.mr.configuration.MapReduceJobConfiguration;
import nsa.datawave.webservice.mr.state.MapReduceStatePersisterBean;
import nsa.datawave.webservice.query.cache.QueryCache;
import nsa.datawave.webservice.query.exception.DatawaveErrorCode;
import nsa.datawave.webservice.query.exception.QueryException;
import nsa.datawave.webservice.query.factory.Persister;
import nsa.datawave.webservice.query.logic.QueryLogicFactory;
import nsa.datawave.webservice.results.mr.MapReduceJobDescription;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.easymock.TestSubject;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.reflect.Whitebox;
import org.springframework.context.support.ClassPathXmlApplicationContext;

@RunWith(EasyMockRunner.class)
@PrepareForTest(Job.class)
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
    private MapReduceStatePersisterBean mapReduceState;
    @Mock
    private ConnectionPoolsConfiguration connectionPoolsConfiguration;
    
    private DatawavePrincipal principal;
    private ClassPathXmlApplicationContext applicationContext;
    
    private static final String userDN = "CN=Guy Some Other soguy, OU=acme";
    private static final String[] auths = new String[] {"AUTHS"};
    
    @Before
    public void setup() throws Exception {
        principal = new DatawavePrincipal(userDN + "<CN=ca, OU=acme>");
        principal.setAuthorizations(principal.getName(), Arrays.asList(auths));
        principal.setUserRoles(principal.getName(), Arrays.asList("AuthorizedUser"));
        
        applicationContext = new ClassPathXmlApplicationContext("classpath:*datawave/mapreduce/MapReduceJobs.xml");
        Whitebox.setInternalState(bean, MapReduceConfiguration.class, applicationContext.getBean(MapReduceConfiguration.class));
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
    
    @Test(expected = BadRequestException.class)
    public void testSubmitInvalidJobName() throws Exception {
        expect(ctx.getCallerPrincipal()).andReturn(principal);
        replayAll();
        
        bean.submit("BadJobName", null);
        verifyAll();
    }
    
    @Test(expected = DatawaveWebApplicationException.class)
    public void testSubmitWithNullRequiredParameters() throws Exception {
        expect(ctx.getCallerPrincipal()).andReturn(principal);
        replayAll();
        
        bean.submit("TestJob", null);
        verifyAll();
    }
    
    @Test(expected = DatawaveWebApplicationException.class)
    public void testSubmitWithMissingRequiredParameters() throws Exception {
        expect(ctx.getCallerPrincipal()).andReturn(principal);
        replayAll();
        
        bean.submit("TestJob", "queryId:1243");
        verifyAll();
    }
    
    @Test(expected = DatawaveWebApplicationException.class)
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
        
        bean.submit("TestJob", "queryId:1243;format:XML");
        verifyAll();
    }
    
    @Test(expected = NoResultsException.class)
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
        
        bean.submit("TestJob", "queryId:1243;format:XML");
        verifyAll();
    }
    
    @Test(expected = UnauthorizedException.class)
    public void testInvalidUserAuthorization() throws Exception {
        // Create principal that does not have AuthorizedUser role
        DatawavePrincipal p = new DatawavePrincipal(userDN + "<CN=ca, OU=acme>");
        p.setAuthorizations(principal.getName(), Arrays.asList(auths));
        p.setUserRoles(principal.getName(), Arrays.asList("Administrator"));
        
        expect(ctx.getCallerPrincipal()).andReturn(p);
        replayAll();
        
        bean.submit("TestJob", "queryId:1243");
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
