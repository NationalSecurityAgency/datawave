package datawave.webservice.mr.configuration;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.hadoop.mapreduce.AccumuloOutputFormat;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.jboss.security.JSSESecurityDomain;

import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.mapreduce.bulkresults.map.BulkResultsTableOutputMapper;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.configuration.QueryData;
import datawave.core.query.logic.QueryLogic;
import datawave.core.query.logic.QueryLogicFactory;
import datawave.microservice.authorization.util.AuthorizationsUtil;
import datawave.microservice.mapreduce.bulkresults.map.SerializationFormat;
import datawave.microservice.query.Query;
import datawave.mr.bulk.BulkInputFormat;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.UserOperations;
import datawave.security.iterator.ConfigurableVisibilityFilter;
import datawave.security.util.WSAuthorizationsUtil;
import datawave.webservice.common.exception.NoResultsException;
import datawave.webservice.mr.bulkresults.map.WeldBulkResultsFileOutputMapper;
import datawave.webservice.query.cache.QueryCache;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.factory.Persister;
import datawave.webservice.query.runner.RunningQuery;

public class BulkResultsJobConfiguration extends MapReduceJobConfiguration implements NeedCallerDetails, NeedAccumuloConnectionFactory, NeedAccumuloDetails,
                NeedQueryLogicFactory, NeedQueryPersister, NeedQueryCache, NeedSecurityDomain {

    /**
     * Container for query settings
     *
     */
    public static class QuerySettings {
        private QueryLogic<?> logic = null;
        private GenericQueryConfiguration queryConfig = null;
        private String base64EncodedQuery = null;
        private Set<Authorizations> runtimeQueryAuthorizations = null;
        private Class<? extends Query> queryImplClass;

        public QuerySettings(QueryLogic<?> logic, GenericQueryConfiguration queryConfig, String base64EncodedQuery, Class<? extends Query> queryImplClass,
                        Set<Authorizations> runtimeQueryAuthorizations) {
            super();
            this.logic = logic;
            this.queryConfig = queryConfig;
            this.base64EncodedQuery = base64EncodedQuery;
            this.queryImplClass = queryImplClass;
            this.runtimeQueryAuthorizations = runtimeQueryAuthorizations;
        }

        public QueryLogic<?> getLogic() {
            return logic;
        }

        public GenericQueryConfiguration getQueryConfig() {
            return queryConfig;
        }

        public String getBase64EncodedQuery() {
            return base64EncodedQuery;
        }

        public Class<? extends Query> getQueryImplClass() {
            return queryImplClass;
        }

        public Set<Authorizations> getRuntimeQueryAuthorizations() {
            return runtimeQueryAuthorizations;
        }
    }

    private Logger log = LogManager.getLogger(this.getClass());

    private JSSESecurityDomain jsseSecurityDomain = null;
    private AccumuloConnectionFactory connectionFactory;
    private QueryLogicFactory queryFactory;
    private UserOperations userOperations;
    private Persister persister;
    private QueryCache runningQueryCache = null;
    private String user;
    private String password;
    private String instanceName;
    private String zookeepers;
    private String sid;
    private Principal principal;

    private String tableName = null;
    private Class<? extends OutputFormat> outputFormatClass = SequenceFileOutputFormat.class;

    @Override
    public void _initializeConfiguration(Job job, Path jobDir, String jobId, Map<String,String> runtimeParameters, DatawavePrincipal serverPrincipal)
                    throws IOException, QueryException {

        String queryId = runtimeParameters.get("queryId");
        SerializationFormat format = SerializationFormat.valueOf(runtimeParameters.get("format"));
        String outputFormatParameter = runtimeParameters.get("outputFormat");
        if (outputFormatParameter != null && outputFormatParameter.equalsIgnoreCase("TEXT")) {
            this.outputFormatClass = TextOutputFormat.class;
        }
        if (runtimeParameters.containsKey("outputTableName"))
            this.tableName = runtimeParameters.get("outputTableName");

        // Initialize the Query
        QueryLogic<?> logic;
        GenericQueryConfiguration queryConfig;
        String base64EncodedQuery;
        Class<? extends Query> queryImplClass;
        Set<Authorizations> runtimeQueryAuthorizations;

        try {
            QuerySettings settings = setupQuery(sid, queryId, principal);
            logic = settings.getLogic();
            queryConfig = settings.getQueryConfig();
            base64EncodedQuery = settings.getBase64EncodedQuery();
            queryImplClass = settings.getQueryImplClass();
            runtimeQueryAuthorizations = settings.getRuntimeQueryAuthorizations();
        } catch (QueryException qe) {
            log.error("Error getting Query for id: " + queryId, qe);
            throw qe;
        } catch (Exception e) {
            log.error("Error setting up Query for id: " + queryId, e);
            throw new QueryException(e);
        }

        // Setup and run the MapReduce job
        try {

            setupJob(job, jobDir, queryConfig, logic, base64EncodedQuery, queryImplClass, runtimeQueryAuthorizations, serverPrincipal);

            if (null == this.tableName) {
                // Setup job for output to HDFS
                // set the mapper
                job.setMapperClass(WeldBulkResultsFileOutputMapper.class);
                job.getConfiguration().set(WeldBulkResultsFileOutputMapper.RESULT_SERIALIZATION_FORMAT, format.name());
                // Setup the output
                job.setOutputFormatClass(outputFormatClass);
                job.setOutputKeyClass(Key.class);
                job.setOutputValueClass(Value.class);
                if (this.outputFormatClass.equals(SequenceFileOutputFormat.class)) {
                    SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
                } else if (this.outputFormatClass.equals(TextOutputFormat.class)) {
                    // if we are writing Text output to hdfs, we don't want to write key-tab-value, we want just the value
                    // this property gets fetched in the Mapper to skip writing the key
                    job.setOutputKeyClass(NullWritable.class);
                }
                job.setNumReduceTasks(0);
                SequenceFileOutputFormat.setOutputPath(job, new Path(this.getResultsDir()));
            } else {
                // Setup job for output to table.
                // set the mapper
                job.setMapperClass(BulkResultsTableOutputMapper.class);
                job.getConfiguration().set(BulkResultsTableOutputMapper.TABLE_NAME, tableName);
                job.getConfiguration().set(WeldBulkResultsFileOutputMapper.RESULT_SERIALIZATION_FORMAT, format.name());
                // Setup the output
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Mutation.class);
                job.setNumReduceTasks(0);
                job.setOutputFormatClass(AccumuloOutputFormat.class);

                // @formatter:off
                Properties clientProps = Accumulo.newClientProperties()
                        .to(instanceName, zookeepers)
                        .as(user, password)
                        .batchWriterConfig(new BatchWriterConfig()
                                .setMaxLatency(30, TimeUnit.SECONDS)
                                .setMaxMemory(10485760)
                                .setMaxWriteThreads(2))
                        .build();

                AccumuloOutputFormat.configure()
                        .clientProperties(clientProps)
                        .createTables(true)
                        .defaultTable(tableName)
                        .store(job);
                // @formatter:on

                // AccumuloOutputFormat.loglevel
                // TODO: this is not supported on the new output format -- just use normal logging configuration
                // AccumuloOutputFormat.setLogLevel(job, Level.INFO);
            }
        } catch (WebApplicationException wex) {
            throw wex;
        } catch (Exception e) {
            log.error("Error starting job", e);
            throw new QueryException(DatawaveErrorCode.JOB_STARTING_ERROR, e);
        }

    }

    /**
     * Common MapReduce setup methods
     *
     * @param job
     *            the job to configure
     * @param jobDir
     *            the directory in HDFS where aux job files are stored
     * @param queryConfig
     *            the query configuration for this job's query input format
     * @param logic
     *            the query logic for this job's query input format
     * @param base64EncodedQuery
     *            the query, encoded using Base64
     * @param queryImplClass
     *            the class of query in {@code base64EncodedQuery}
     * @param runtimeQueryAuthorizations
     *            the authorizations to use for input format query scanners
     * @param serverPrincipal
     *            the {@link Principal} of the server running DATAWAVE
     * @throws IOException
     *             for IOException
     * @throws AccumuloSecurityException
     *             for AccumuloSecurityException
     */
    private void setupJob(Job job, Path jobDir, GenericQueryConfiguration queryConfig, QueryLogic<?> logic, String base64EncodedQuery,
                    Class<? extends Query> queryImplClass, Set<Authorizations> runtimeQueryAuthorizations, DatawavePrincipal serverPrincipal)
                    throws IOException, AccumuloSecurityException {

        job.setInputFormatClass(BulkInputFormat.class);

        QueryData queryData = null;
        Collection<Range> ranges = new ArrayList<>();

        if (!queryConfig.canRunQuery()) {
            throw new UnsupportedOperationException("Unable to run query");
        }

        Iterator<QueryData> iter = queryConfig.getQueriesIter();
        while (iter.hasNext()) {
            queryData = iter.next();
            ranges.addAll(queryData.getRanges());
        }

        if (ranges.isEmpty()) {
            throw new NoResultsException(new QueryException("No scan ranges produced for query."));
        }

        BulkInputFormat.setWorkingDirectory(job.getConfiguration(), jobDir.toString());

        // Copy the information from the GenericQueryConfiguration to the job.
        BulkInputFormat.setRanges(job, ranges);

        for (IteratorSetting cfg : queryData.getSettings()) {
            BulkInputFormat.addIterator(job.getConfiguration(), cfg);
        }

        BulkInputFormat.setZooKeeperInstance(job.getConfiguration(), this.instanceName, this.zookeepers);
        Iterator<Authorizations> authsIter = (runtimeQueryAuthorizations == null || runtimeQueryAuthorizations.isEmpty()) ? null
                        : runtimeQueryAuthorizations.iterator();
        Authorizations auths = (authsIter == null) ? null : authsIter.next();
        BulkInputFormat.setInputInfo(job, this.user, this.password.getBytes(), logic.getTableName(), auths);
        for (int priority = 10; authsIter != null && authsIter.hasNext(); ++priority) {
            IteratorSetting cfg = new IteratorSetting(priority, ConfigurableVisibilityFilter.class);
            cfg.setName("visibilityFilter" + priority);
            cfg.addOption(ConfigurableVisibilityFilter.AUTHORIZATIONS_OPT, authsIter.next().toString());
            BulkInputFormat.addIterator(job.getConfiguration(), cfg);
        }

        job.getConfiguration().set(WeldBulkResultsFileOutputMapper.QUERY_LOGIC_SETTINGS, base64EncodedQuery);
        job.getConfiguration().set(WeldBulkResultsFileOutputMapper.QUERY_IMPL_CLASS, queryImplClass.getName());
        job.getConfiguration().set(WeldBulkResultsFileOutputMapper.QUERY_LOGIC_NAME, logic.getLogicName());

        job.getConfiguration().set(WeldBulkResultsFileOutputMapper.APPLICATION_CONTEXT_PATH,
                        "classpath*:datawave/configuration/spring/CDIBeanPostProcessor.xml," + "classpath*:datawave/query/*QueryLogicFactory.xml,"
                                        + "classpath*:/MarkingFunctionsContext.xml," + "classpath*:/MetadataHelperContext.xml,"
                                        + "classpath*:/CacheContext.xml");
        job.getConfiguration().set(WeldBulkResultsFileOutputMapper.SPRING_CONFIG_LOCATIONS,
                        job.getConfiguration().get(WeldBulkResultsFileOutputMapper.APPLICATION_CONTEXT_PATH));
        // Tell the Mapper/Reducer to use a specific set of application context files when doing Spring-CDI integration.
        String cdiOpts = "'-Dcdi.spring.configs=" + job.getConfiguration().get(WeldBulkResultsFileOutputMapper.APPLICATION_CONTEXT_PATH) + "'";
        // Pass our server DN along to the child VM so it can be made available for injection.
        cdiOpts += " '-Dserver.principal=" + encodePrincipal(serverPrincipal) + "'";
        cdiOpts += " '-Dcaller.principal=" + encodePrincipal((DatawavePrincipal) principal) + "'";
        String javaOpts = job.getConfiguration().get("mapreduce.map.java.opts");
        javaOpts = (javaOpts == null) ? cdiOpts : (javaOpts + " " + cdiOpts);
        job.getConfiguration().set("mapreduce.map.java.opts", javaOpts);
        job.setMapOutputKeyClass(Key.class);
        job.setMapOutputValueClass(Value.class);

        job.setWorkingDirectory(jobDir);
    }

    private String encodePrincipal(DatawavePrincipal principal) throws IOException {

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); ObjectOutputStream oos = new ObjectOutputStream(baos)) {

            // create a copy because this DatawavePrincipal might be CDI injected and have a reference to Weld
            oos.writeObject(new DatawavePrincipal(principal.getProxiedUsers(), principal.getCreationTime()));
            return Base64.encodeBase64String(baos.toByteArray());
        }
    }

    private QuerySettings setupQuery(String sid, String queryId, Principal principal) throws Exception {

        AccumuloClient client = null;
        QueryLogic<?> logic = null;
        try {
            // Get the query by the query id
            Query q = getQueryById(queryId);
            if (!sid.equals(q.getOwner()))
                throw new QueryException("This query does not belong to you. expected: " + q.getOwner() + ", value: " + sid,
                                Response.Status.UNAUTHORIZED.getStatusCode());

            String userDN = null;
            Collection<String> proxyServers = null;
            if (principal instanceof DatawavePrincipal) {
                DatawavePrincipal dp = (DatawavePrincipal) principal;
                userDN = dp.getUserDN().subjectDN();
                proxyServers = dp.getProxyServers();
            }

            // will throw IllegalArgumentException if not defined
            logic = queryFactory.getQueryLogic(q.getQueryLogicName(), (DatawavePrincipal) principal);

            // Get an accumulo connection
            Map<String,String> trackingMap = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
            client = connectionFactory.getClient(userDN, proxyServers, logic.getConnectionPriority(), trackingMap);

            if (q.getQueryAuthorizations() == null) {
                logic.preInitialize(q, AuthorizationsUtil.buildAuthorizations(null));
            } else {
                logic.preInitialize(q,
                                AuthorizationsUtil.buildAuthorizations(Collections.singleton(AuthorizationsUtil.splitAuths(q.getQueryAuthorizations()))));
            }
            // Merge user auths with the auths that they use in the Query
            // the query principal is our local principal unless the query logic has a different user operations
            DatawavePrincipal queryPrincipal = (DatawavePrincipal) ((logic.getUserOperations() == null) ? principal
                            : logic.getUserOperations().getRemoteUser((DatawavePrincipal) principal));
            // the overall principal (the one with combined auths across remote user operations) is our own user operations (probably the UserOperationsBean)
            DatawavePrincipal overallPrincipal = (DatawavePrincipal) ((userOperations == null) ? principal
                            : userOperations.getRemoteUser((DatawavePrincipal) principal));
            Set<Authorizations> runtimeQueryAuthorizations = WSAuthorizationsUtil.getDowngradedAuthorizations(q.getQueryAuthorizations(), overallPrincipal,
                            queryPrincipal);

            // Initialize the logic so that the configuration contains all of the iterator options
            GenericQueryConfiguration queryConfig = logic.initialize(client, q, runtimeQueryAuthorizations);

            String base64EncodedQuery = WeldBulkResultsFileOutputMapper.serializeQuery(q);

            return new QuerySettings(logic, queryConfig, base64EncodedQuery, q.getClass(), runtimeQueryAuthorizations);
        } finally {
            if (null != logic && null != client)
                connectionFactory.returnClient(client);
        }

    }

    private Query getQueryById(String id) throws QueryException {

        RunningQuery runningQuery = runningQueryCache.get(id);
        if (null != runningQuery) {
            return runningQuery.getSettings();
        } else {
            List<Query> queries = persister.findById(id);
            if (null == queries || queries.isEmpty())
                throw new QueryException("No query object matches this id", Response.Status.NOT_FOUND.getStatusCode());
            if (queries.size() > 1)
                throw new QueryException("More than one query object matches the id", Response.Status.NOT_FOUND.getStatusCode());
            return queries.get(0);
        }
    }

    @Override
    public void setSecurityDomain(JSSESecurityDomain jsseSecurityDomain) {
        this.jsseSecurityDomain = jsseSecurityDomain;
    }

    public void setUserOperations(UserOperations userOperations) {
        this.userOperations = userOperations;
    }

    protected void exportSystemProperties(String jobId, Job job, FileSystem fs, Path classpath) {
        Properties systemProperties = new Properties();
        systemProperties.putAll(System.getProperties());
        if (this.jobSystemProperties != null) {
            systemProperties.putAll(this.jobSystemProperties);
        }

        if (this.jsseSecurityDomain != null) {
            String useJobCacheString = systemProperties.getProperty("dw.mapreduce.securitydomain.useJobCache");
            boolean useJobCache = Boolean.parseBoolean(useJobCacheString);
            if (useJobCache) {
                try {
                    String keyStoreURL = systemProperties.getProperty("dw.mapreduce.securitydomain.keyStoreURL");
                    File keyStore = new File(keyStoreURL);
                    Path keyStorePath = new Path(classpath, keyStore.getName());
                    addSingleFile(keyStoreURL, keyStorePath, jobId, job, fs);
                    systemProperties.setProperty("dw.mapreduce.securitydomain.keyStoreURL", keyStore.getName());
                } catch (IOException e) {
                    log.error(e.getMessage(), e);
                }
                try {
                    String trustStoreURL = systemProperties.getProperty("dw.mapreduce.securitydomain.trustStoreURL");
                    File trustStore = new File(trustStoreURL);
                    Path trustStorePath = new Path(classpath, trustStore.getName());
                    addSingleFile(trustStoreURL, trustStorePath, jobId, job, fs);
                    systemProperties.setProperty("dw.mapreduce.securitydomain.trustStoreURL", trustStore.getName());
                } catch (IOException e) {
                    log.error(e.getMessage(), e);
                }
            }

            if (jsseSecurityDomain.getClientAlias() != null) {
                systemProperties.setProperty("dw.mapreduce.securitydomain.clientAlias", jsseSecurityDomain.getClientAlias());
            }
            if (jsseSecurityDomain.getServerAlias() != null) {
                systemProperties.setProperty("dw.mapreduce.securitydomain.serverAlias", jsseSecurityDomain.getServerAlias());
            }
            if (jsseSecurityDomain.getCipherSuites() != null) {
                systemProperties.setProperty("dw.mapreduce.securitydomain.cipherSuites", StringUtils.join(jsseSecurityDomain.getCipherSuites(), ','));
            }
            if (jsseSecurityDomain.getProtocols() != null) {
                systemProperties.setProperty("dw.mapreduce.securitydomain.protocols", StringUtils.join(jsseSecurityDomain.getProtocols(), ','));
            }
            systemProperties.setProperty("dw.mapreduce.securitydomain.clientAuth", Boolean.toString(jsseSecurityDomain.isClientAuth()));
        }
        writeProperties(jobId, job, fs, classpath, systemProperties);
    }

    @Override
    public void setQueryLogicFactory(QueryLogicFactory factory) {
        this.queryFactory = factory;
    }

    @Override
    public void setUsername(String username) {
        this.user = username;
    }

    @Override
    public void setPassword(String password) {
        this.password = password;
    }

    @Override
    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }

    @Override
    public void setZookeepers(String zookeepers) {
        this.zookeepers = zookeepers;
    }

    @Override
    public void setAccumuloConnectionFactory(AccumuloConnectionFactory factory) {
        this.connectionFactory = factory;
    }

    @Override
    public void setUserSid(String sid) {
        this.sid = sid;
    }

    @Override
    public void setPrincipal(Principal principal) {
        this.principal = principal;
    }

    @Override
    public void setPersister(Persister persister) {
        this.persister = persister;
    }

    @Override
    public void setQueryCache(QueryCache cache) {
        this.runningQueryCache = cache;
    }
}
