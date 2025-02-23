package datawave.microservice.query.mapreduce.jobs;

import static datawave.microservice.query.mapreduce.config.MapReduceQueryProperties.FORMAT;
import static datawave.microservice.query.mapreduce.config.MapReduceQueryProperties.OUTPUT_FORMAT;
import static datawave.microservice.query.mapreduce.config.MapReduceQueryProperties.OUTPUT_TABLE_NAME;
import static datawave.microservice.query.mapreduce.config.MapReduceQueryProperties.QUERY_ID;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.WebApplicationException;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.hadoop.mapreduce.AccumuloOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.mapreduce.bulkresults.map.BulkResultsFileOutputMapper;
import datawave.core.mapreduce.bulkresults.map.BulkResultsTableOutputMapper;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.configuration.QueryData;
import datawave.core.query.logic.QueryLogic;
import datawave.core.query.logic.QueryLogicFactory;
import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.microservice.authorization.util.AuthorizationsUtil;
import datawave.microservice.mapreduce.bulkresults.map.SerializationFormat;
import datawave.microservice.query.Query;
import datawave.microservice.query.mapreduce.config.MapReduceQueryProperties;
import datawave.microservice.query.mapreduce.status.MapReduceQueryStatus;
import datawave.mr.bulk.BulkInputFormat;
import datawave.query.exceptions.NoResultsException;
import datawave.query.iterator.QueryOptions;
import datawave.security.authorization.UserOperations;
import datawave.security.iterator.ConfigurableVisibilityFilter;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;

public class BulkResultsJob extends MapReduceJob {
    
    private final Map<String,String> propertiesMap;
    
    private UserOperations userOperations;
    
    /**
     * Container for query settings
     */
    public static class QuerySettings {
        private final QueryLogic<?> logic;
        private final GenericQueryConfiguration queryConfig;
        private final String base64EncodedQuery;
        private final Set<Authorizations> runtimeQueryAuthorizations;
        private final Class<? extends Query> queryImplClass;
        
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
    
    private String tableName = null;
    private Class<? extends OutputFormat> outputFormatClass = SequenceFileOutputFormat.class;
    
    public BulkResultsJob(MapReduceQueryProperties mapReduceQueryProperties, Map<String,String> propertiesMap) {
        super(mapReduceQueryProperties);
        this.propertiesMap = propertiesMap;
    }
    
    @Override
    public void _initializeConfiguration(QueryLogicFactory queryLogicFactory, AccumuloConnectionFactory accumuloConnectionFactory,
                    MapReduceQueryStatus mapReduceQueryStatus, Job job, Path jobDir) throws QueryException {
        
        String queryId = mapReduceQueryStatus.getParameters().getFirst(QUERY_ID);
        SerializationFormat format = SerializationFormat.valueOf(mapReduceQueryStatus.getParameters().getFirst(FORMAT));
        String outputFormatParameter = mapReduceQueryStatus.getParameters().getFirst(OUTPUT_FORMAT);
        if (outputFormatParameter != null && outputFormatParameter.equalsIgnoreCase("TEXT")) {
            this.outputFormatClass = TextOutputFormat.class;
        }
        if (mapReduceQueryStatus.getParameters().containsKey(OUTPUT_TABLE_NAME)) {
            this.tableName = mapReduceQueryStatus.getParameters().getFirst(OUTPUT_TABLE_NAME);
        }
        
        // Initialize the Query
        QueryLogic<?> logic;
        GenericQueryConfiguration queryConfig;
        String base64EncodedQuery;
        Class<? extends Query> queryImplClass;
        Set<Authorizations> runtimeQueryAuthorizations;
        
        try {
            QuerySettings settings = setupQuery(queryLogicFactory, accumuloConnectionFactory, mapReduceQueryStatus);
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
            setupJob(job, jobDir, queryConfig, logic, base64EncodedQuery, queryImplClass, runtimeQueryAuthorizations);
            
            if (null == this.tableName) {
                // Setup job for output to HDFS
                // set the mapper
                job.setMapperClass(BulkResultsFileOutputMapper.class);
                job.getConfiguration().set(BulkResultsFileOutputMapper.RESULT_SERIALIZATION_FORMAT, format.name());
                // Set up the output
                job.setOutputFormatClass(outputFormatClass);
                job.setOutputKeyClass(Key.class);
                job.setOutputValueClass(Value.class);
                if (this.outputFormatClass.equals(SequenceFileOutputFormat.class)) {
                    SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
                } else if (this.outputFormatClass.equals(TextOutputFormat.class)) {
                    // if we are writing Text output to hdfs, we don't want to write key-tab-value, we want just the value
                    // this property gets fetched in the Mapper to skip writing the key
                    job.setOutputKeyClass(NullWritable.class);
                }
                job.setNumReduceTasks(0);
                SequenceFileOutputFormat.setOutputPath(job, new Path(mapReduceQueryStatus.getResultsDirectory()));
            } else {
                // Setup job for output to table.
                // set the mapper
                job.setMapperClass(BulkResultsTableOutputMapper.class);
                job.getConfiguration().set(BulkResultsTableOutputMapper.TABLE_NAME, tableName);
                job.getConfiguration().set(BulkResultsFileOutputMapper.RESULT_SERIALIZATION_FORMAT, format.name());
                // Set up the output
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Mutation.class);
                job.setNumReduceTasks(0);
                job.setOutputFormatClass(AccumuloOutputFormat.class);
                
                // formatter:off
                Properties clientProps = Accumulo.newClientProperties()
                                .to(mapReduceJobProperties.getAccumulo().getInstanceName(), mapReduceJobProperties.getAccumulo().getZookeepers())
                                .as(mapReduceJobProperties.getAccumulo().getUsername(), mapReduceJobProperties.getAccumulo().getPassword())
                                .batchWriterConfig(new BatchWriterConfig().setMaxLatency(30, TimeUnit.SECONDS).setMaxMemory(10485760).setMaxWriteThreads(2))
                                .build();
                
                AccumuloOutputFormat.configure().clientProperties(clientProps).createTables(true).defaultTable(tableName).store(job);
                // formatter:on
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
     * @throws IOException
     *             if there is an issue setting the job working directory
     * @throws NoResultsException
     *             if there are no query ranges to process
     */
    private void setupJob(Job job, Path jobDir, GenericQueryConfiguration queryConfig, QueryLogic<?> logic, String base64EncodedQuery,
                    Class<? extends Query> queryImplClass, Set<Authorizations> runtimeQueryAuthorizations) throws IOException, NoResultsException {
        
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
            // Note: We can't use the query from the first query data as it has likely pruned fields which don't exist within that range
            // Ideally, we would use the plan specific to each range, but at a minimum we can use the base query plan
            if (cfg.getOptions().containsKey(QueryOptions.QUERY)) {
                cfg.addOption(QueryOptions.QUERY, queryConfig.getQueryString());
            }
            BulkInputFormat.addIterator(job.getConfiguration(), cfg);
        }
        
        BulkInputFormat.setZooKeeperInstance(job.getConfiguration(), mapReduceJobProperties.getAccumulo().getInstanceName(),
                        mapReduceJobProperties.getAccumulo().getZookeepers());
        Iterator<Authorizations> authsIter = (runtimeQueryAuthorizations == null || runtimeQueryAuthorizations.isEmpty()) ? null
                        : runtimeQueryAuthorizations.iterator();
        Authorizations auths = (authsIter == null) ? null : authsIter.next();
        BulkInputFormat.setInputInfo(job, mapReduceJobProperties.getAccumulo().getUsername(),
                        mapReduceJobProperties.getAccumulo().getPassword().getBytes(StandardCharsets.UTF_8), logic.getTableName(), auths);
        for (int priority = 10; authsIter != null && authsIter.hasNext(); ++priority) {
            IteratorSetting cfg = new IteratorSetting(priority, ConfigurableVisibilityFilter.class);
            cfg.setName("visibilityFilter" + priority);
            cfg.addOption(ConfigurableVisibilityFilter.AUTHORIZATIONS_OPT, authsIter.next().toString());
            BulkInputFormat.addIterator(job.getConfiguration(), cfg);
        }
        
        job.getConfiguration().set(BulkResultsFileOutputMapper.QUERY_LOGIC_SETTINGS, base64EncodedQuery);
        job.getConfiguration().set(BulkResultsFileOutputMapper.QUERY_IMPL_CLASS, queryImplClass.getName());
        job.getConfiguration().set(BulkResultsFileOutputMapper.QUERY_LOGIC_NAME, logic.getLogicName());
        
        if (mapReduceJobProperties.getBasePackages() != null && !mapReduceJobProperties.getBasePackages().isEmpty()) {
            job.getConfiguration().set(BulkResultsFileOutputMapper.SPRING_CONFIG_BASE_PACKAGES, String.join(",", mapReduceJobProperties.getBasePackages()));
        }
        
        if (mapReduceJobProperties.getStartingClass() != null && !mapReduceJobProperties.getStartingClass().isEmpty()) {
            job.getConfiguration().set(BulkResultsFileOutputMapper.SPRING_CONFIG_STARTING_CLASS, mapReduceJobProperties.getStartingClass());
        }
        
        String javaOpts = job.getConfiguration().get("mapreduce.map.java.opts");
        job.getConfiguration().set("mapreduce.map.java.opts", javaOpts);
        job.setMapOutputKeyClass(Key.class);
        job.setMapOutputValueClass(Value.class);
        
        job.setWorkingDirectory(jobDir);
    }
    
    private QuerySettings setupQuery(QueryLogicFactory queryLogicFactory, AccumuloConnectionFactory accumuloConnectionFactory,
                    MapReduceQueryStatus mapReduceQueryStatus) throws Exception {
        AccumuloClient client = null;
        QueryLogic<?> logic = null;
        try {
            String userDN = mapReduceQueryStatus.getCurrentUser().getPrimaryUser().getDn().subjectDN();
            Collection<String> proxyServers = mapReduceQueryStatus.getCurrentUser().getProxyServers();
            
            // will throw IllegalArgumentException if not defined
            logic = queryLogicFactory.getQueryLogic(mapReduceQueryStatus.getQuery().getQueryLogicName(), mapReduceQueryStatus.getCurrentUser());
            
            // Get an accumulo connection
            Map<String,String> trackingMap = accumuloConnectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
            client = accumuloConnectionFactory.getClient(userDN, proxyServers, logic.getConnectionPriority(), trackingMap);
            
            Query query = mapReduceQueryStatus.getQuery();
            if (query.getQueryAuthorizations() == null) {
                logic.preInitialize(query, AuthorizationsUtil.buildAuthorizations(null));
            } else {
                logic.preInitialize(query,
                                AuthorizationsUtil.buildAuthorizations(Collections.singleton(AuthorizationsUtil.splitAuths(query.getQueryAuthorizations()))));
            }
            // Merge user auths with the auths that they use in the Query
            // the query principal is our local principal unless the query logic has a different user operations
            DatawaveUserDetails queryUserDetails = (DatawaveUserDetails) ((logic.getUserOperations() == null) ? mapReduceQueryStatus.getCurrentUser()
                            : logic.getUserOperations().getRemoteUser(mapReduceQueryStatus.getCurrentUser()));
            // the overall principal (the one with combined auths across remote user operations) is our own user operations (probably the UserOperationsBean)
            DatawaveUserDetails overallUserDetails = (DatawaveUserDetails) ((userOperations == null) ? mapReduceQueryStatus.getCurrentUser()
                            : userOperations.getRemoteUser(mapReduceQueryStatus.getCurrentUser()));
            Set<Authorizations> runtimeQueryAuthorizations = AuthorizationsUtil.getDowngradedAuthorizations(query.getQueryAuthorizations(), overallUserDetails,
                            queryUserDetails);
            
            // Initialize the logic so that the configuration contains all of the iterator options
            GenericQueryConfiguration queryConfig = logic.initialize(client, query, runtimeQueryAuthorizations);
            
            String base64EncodedQuery = BulkResultsFileOutputMapper.serializeQuery(query);
            
            return new QuerySettings(logic, queryConfig, base64EncodedQuery, query.getClass(), runtimeQueryAuthorizations);
        } finally {
            if (null != logic && null != client)
                accumuloConnectionFactory.returnClient(client);
        }
    }
    
    @Override
    protected void exportSystemProperties(String id, Job job, FileSystem fs, Path classpath) {
        Properties systemProperties = new Properties();
        systemProperties.putAll(propertiesMap);
        if (mapReduceJobProperties.getJobSystemProperties() != null) {
            systemProperties.putAll(mapReduceJobProperties.getJobSystemProperties());
        }
        
        // Note: The logic used to submit a mapreduce query is not needed when running in the mapper
        systemProperties.put(MapReduceQueryProperties.PREFIX + ".enabled", "false");
        
        writeProperties(id, job, fs, classpath, systemProperties);
    }
    
    public void setUserOperations(UserOperations userOperations) {
        this.userOperations = userOperations;
    }
}
