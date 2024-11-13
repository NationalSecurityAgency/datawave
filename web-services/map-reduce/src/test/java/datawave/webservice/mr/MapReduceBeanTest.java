package datawave.webservice.mr;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.ejb.EJBContext;

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

import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.query.logic.QueryLogicFactory;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.DatawaveUser.UserType;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.util.DnUtils;
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
import datawave.webservice.results.mr.MapReduceJobDescription;

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
        System.setProperty(DnUtils.NPE_OU_PROPERTY, "iamnotaperson");
        System.setProperty("dw.metadatahelper.all.auths", "A,B,C,D");
        DatawaveUser user = new DatawaveUser(SubjectIssuerDNPair.of(userDN, "CN=ca, OU=acme"), UserType.USER, Arrays.asList(auths),
                        Collections.singleton("AuthorizedUser"), null, 0L);
        principal = new DatawavePrincipal(Collections.singletonList(user));

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
        DatawaveUser user = new DatawaveUser(SubjectIssuerDNPair.of(userDN, "CN=ca, OU=acme"), UserType.USER, Arrays.asList(auths),
                        Collections.singleton("Administrator"), null, 0L);
        DatawavePrincipal p = new DatawavePrincipal(Collections.singletonList(user));

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
