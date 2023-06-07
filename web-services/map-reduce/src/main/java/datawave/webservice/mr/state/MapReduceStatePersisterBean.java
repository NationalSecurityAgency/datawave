package datawave.webservice.mr.state;

import datawave.configuration.DatawaveEmbeddedProjectStageHolder;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.util.ScannerHelper;
import datawave.webservice.common.connection.AccumuloConnectionFactory;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.NotFoundQueryException;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.results.mr.JobExecution;
import datawave.webservice.results.mr.MapReduceInfoResponse;
import datawave.webservice.results.mr.MapReduceInfoResponseList;
import datawave.webservice.results.mr.ResultFile;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.deltaspike.core.api.exclude.Exclude;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import javax.annotation.Resource;
import javax.annotation.security.DeclareRoles;
import javax.annotation.security.PermitAll;
import javax.annotation.security.RolesAllowed;
import javax.ejb.EJBContext;
import javax.ejb.LocalBean;
import javax.ejb.Stateless;
import javax.inject.Inject;
import java.io.IOException;
import java.security.Principal;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

/**
 * Maintains information in a table about the state of a MapReduce job <br>
 *
 * MapReduce Table
 * <table>
 * <caption></caption>
 * <tr>
 * <th>Row</th>
 * <th>ColF</th>
 * <th>ColQ</th>
 * <th>Value</th>
 * </tr>
 * <tr>
 * <td>id</td>
 * <td>sid</td>
 * <td>dir</td>
 * <td>Job Working Directory</td>
 * </tr>
 * <tr>
 * <td>id</td>
 * <td>sid</td>
 * <td>hdfs</td>
 * <td>hdfs uri</td>
 * </tr>
 * <tr>
 * <td>id</td>
 * <td>sid</td>
 * <td>jt</td>
 * <td>job tracker</td>
 * </tr>
 * <tr>
 * <td>id</td>
 * <td>sid</td>
 * <td>name</td>
 * <td>job name</td>
 * </tr>
 * <tr>
 * <td>id</td>
 * <td>sid</td>
 * <td>params</td>
 * <td>runtime parameters</td>
 * </tr>
 * <tr>
 * <td>id</td>
 * <td>sid</td>
 * <td>results</td>
 * <td>results directory in HDFS or NULL</td>
 * </tr>
 * <tr>
 * <td>id</td>
 * <td>sid</td>
 * <td>state\0jobId</td>
 * <td>STARTED|SUCCEEDED|FAILED|PREP|KILLED</td>
 * </tr>
 * </table>
 *
 * MapReduce Index
 * <table>
 * <caption></caption>
 * <tr>
 * <th>Row</th>
 * <th>ColF</th>
 * <th>ColQ</th>
 * <th>Value</th>
 * </tr>
 * <tr>
 * <td>jobId</td>
 * <td>sid</td>
 * <td>id</td>
 * <td>NULL</td>
 * </tr>
 * </table>
 *
 */
@RolesAllowed({"AuthorizedUser", "AuthorizedQueryServer", "InternalUser", "Administrator"})
@DeclareRoles({"AuthorizedUser", "AuthorizedQueryServer", "InternalUser", "Administrator"})
@LocalBean
@Stateless
@Exclude(ifProjectStage = DatawaveEmbeddedProjectStageHolder.DatawaveEmbedded.class)
public class MapReduceStatePersisterBean {
    public enum MapReduceState {
        STARTED, RUNNING, SUCCEEDED, FAILED, KILLED
    }
    
    private Logger log = Logger.getLogger(this.getClass());
    
    @Inject
    private AccumuloConnectionFactory connectionFactory;
    
    @Resource
    private EJBContext ctx;
    
    public static final String TABLE_NAME = "MapReduceService";
    public static final String INDEX_TABLE_NAME = "MapReduceServiceJobIndex";
    public static final String WORKING_DIRECTORY = "dir";
    public static final String RESULTS_LOCATION = "results";
    public static final String PARAMS = "params";
    public static final String HDFS = "hdfs";
    public static final String JT = "jt";
    public static final String STATE = "state";
    public static final String NAME = "name";
    public static final String NULL = "\u0000";
    public static final Value NULL_VALUE = new Value(new byte[0]);
    
    /**
     *
     * @param id
     *            map reduce id
     * @param hdfsUri
     *            the uri
     * @param jobTracker
     *            the job tracker name
     * @param workingDirectory
     *            map reduce job working directory
     * @param mapReduceJobId
     *            map reduce job id
     * @param resultsDirectory
     *            either HDFS directory name or some other location (i.e. table name)
     * @param runtimeParameters
     *            parameters
     * @param jobName
     *            job name
     * @throws QueryException
     *             for QueryException
     */
    public void create(String id, String hdfsUri, String jobTracker, String workingDirectory, String mapReduceJobId, String resultsDirectory,
                    String runtimeParameters, String jobName) throws QueryException {
        // Find out who/what called this method
        Principal p = ctx.getCallerPrincipal();
        String sid = p.getName();
        if (p instanceof DatawavePrincipal) {
            DatawavePrincipal cp = (DatawavePrincipal) p;
            sid = cp.getShortName();
        }
        
        AccumuloClient c = null;
        try {
            Map<String,String> trackingMap = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
            c = connectionFactory.getClient(AccumuloConnectionFactory.Priority.ADMIN, trackingMap);
            tableCheck(c);
            // Not using a MultiTableBatchWriter here because its not implemented yet
            // in Mock Accumulo.
            BatchWriterConfig bwCfg = new BatchWriterConfig().setMaxLatency(10, TimeUnit.SECONDS).setMaxMemory(10240L).setMaxWriteThreads(1);
            try (BatchWriter tableWriter = c.createBatchWriter(TABLE_NAME, bwCfg); BatchWriter indexWriter = c.createBatchWriter(INDEX_TABLE_NAME, bwCfg)) {
                Mutation m = new Mutation(id);
                m.put(sid, WORKING_DIRECTORY, workingDirectory);
                m.put(sid, HDFS, hdfsUri);
                m.put(sid, JT, jobTracker);
                m.put(sid, NAME, jobName);
                m.put(sid, RESULTS_LOCATION, resultsDirectory);
                m.put(sid, PARAMS, runtimeParameters);
                m.put(sid, STATE + NULL + mapReduceJobId, new Value(MapReduceState.STARTED.toString().getBytes()));
                tableWriter.addMutation(m);
                Mutation i = new Mutation(mapReduceJobId);
                i.put(sid, id, NULL_VALUE);
                indexWriter.addMutation(i);
            }
        } catch (Exception e) {
            QueryException qe = new QueryException(DatawaveErrorCode.BULK_RESULTS_ENTRY_ERROR, e);
            log.error(qe);
            throw qe;
        } finally {
            try {
                connectionFactory.returnClient(c);
            } catch (Exception e) {
                log.error("Error closing writers", e);
            }
        }
    }
    
    /**
     * Update the state of the Bulk Results entry
     *
     * @param mapReduceJobId
     *            job id
     * @param state
     *            new state
     * @throws QueryException
     *             when zero or more than one result is found for the id
     */
    @PermitAll
    public void updateState(String mapReduceJobId, MapReduceState state) throws QueryException {
        // We have the mapreduce job id and the new state, but we need to find out which id and sid this relates to
        // so that we can create a new mutation to put into the table.
        List<MapReduceServiceJobIndex> results = null;
        // Find the index entry for the jobid
        AccumuloClient c = null;
        try {
            Map<String,String> trackingMap = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
            c = connectionFactory.getClient(AccumuloConnectionFactory.Priority.ADMIN, trackingMap);
            tableCheck(c);
            try (Scanner scanner = ScannerHelper.createScanner(c, INDEX_TABLE_NAME, Collections.singleton(new Authorizations()))) {
                Range range = new Range(mapReduceJobId, mapReduceJobId);
                scanner.setRange(range);
                
                for (Entry<Key,Value> entry : scanner) {
                    if (null == results)
                        results = new ArrayList<>();
                    results.add(MapReduceServiceJobIndex.parse(entry.getKey(), state));
                }
            }
        } catch (Exception e) {
            QueryException qe = new QueryException(DatawaveErrorCode.JOB_ID_LOOKUP_ERROR, e, MessageFormat.format("job_id: {0}", mapReduceJobId));
            log.error(qe);
            throw qe;
        } finally {
            try {
                connectionFactory.returnClient(c);
            } catch (Exception e) {
                log.error("Error returning connection to pool", e);
            }
        }
        
        if (null == results)
            throw new NotFoundQueryException(DatawaveErrorCode.NO_QUERY_OBJECT_MATCH);
        if (results.size() > 1)
            throw new NotFoundQueryException(DatawaveErrorCode.TOO_MANY_QUERY_OBJECT_MATCHES);
        else {
            MapReduceServiceJobIndex r = results.get(0);
            // We will insert a new history column in the table
            Mutation m = new Mutation(r.getId());
            m.put(r.getUser(), STATE + NULL + r.getMapReduceJobId(), new Value(r.getState().getBytes()));
            c = null;
            BatchWriter writer = null;
            try {
                Map<String,String> trackingMap = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
                c = connectionFactory.getClient(AccumuloConnectionFactory.Priority.ADMIN, trackingMap);
                tableCheck(c);
                writer = c.createBatchWriter(TABLE_NAME,
                                new BatchWriterConfig().setMaxLatency(10, TimeUnit.SECONDS).setMaxMemory(10240L).setMaxWriteThreads(1));
                writer.addMutation(m);
                writer.flush();
            } catch (RuntimeException re) {
                throw re;
            } catch (Exception e) {
                QueryException qe = new QueryException(DatawaveErrorCode.JOB_STATE_UPDATE_ERROR, e, MessageFormat.format("job_id: {0}", mapReduceJobId));
                log.error(qe);
                throw qe;
            } finally {
                try {
                    if (null != writer)
                        writer.close();
                    connectionFactory.returnClient(c);
                } catch (Exception e) {
                    log.error("Error creating query", e);
                }
            }
        }
    }
    
    /**
     * Returns all MapReduce jobs for the current user
     *
     * @return list of map reduce information
     */
    public MapReduceInfoResponseList find() {
        // Find out who/what called this method
        Principal p = ctx.getCallerPrincipal();
        String sid = p.getName();
        Set<Authorizations> auths = new HashSet<>();
        if (p instanceof DatawavePrincipal) {
            DatawavePrincipal dp = (DatawavePrincipal) p;
            sid = dp.getShortName();
            for (Collection<String> cbAuths : dp.getAuthorizations())
                auths.add(new Authorizations(cbAuths.toArray(new String[cbAuths.size()])));
        }
        log.trace(sid + " has authorizations " + auths);
        
        MapReduceInfoResponseList result = new MapReduceInfoResponseList();
        AccumuloClient c = null;
        try {
            Map<String,String> trackingMap = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
            c = connectionFactory.getClient(AccumuloConnectionFactory.Priority.ADMIN, trackingMap);
            tableCheck(c);
            try (Scanner scanner = ScannerHelper.createScanner(c, TABLE_NAME, auths)) {
                scanner.fetchColumnFamily(new Text(sid));
                
                // We need to create a response for each job
                String previousRow = sid;
                Map<Key,Value> batch = new HashMap<>();
                for (Entry<Key,Value> entry : scanner) {
                    if (!previousRow.equals(entry.getKey().getRow().toString()) && !batch.isEmpty()) {
                        MapReduceInfoResponse response = populateResponse(batch.entrySet());
                        if (null != response)
                            result.getResults().add(response);
                        batch.clear();
                    } else {
                        batch.put(entry.getKey(), entry.getValue());
                    }
                    previousRow = entry.getKey().getRow().toString();
                }
                if (!batch.isEmpty()) {
                    MapReduceInfoResponse response = populateResponse(batch.entrySet());
                    if (null != response)
                        result.getResults().add(response);
                    batch.clear();
                }
                return result;
            }
        } catch (IOException ioe) {
            QueryException qe = new QueryException(DatawaveErrorCode.RESPONSE_POPULATION_ERROR, ioe);
            log.error(qe);
            result.addException(qe);
            return result;
        } catch (Exception e) {
            QueryException qe = new QueryException(DatawaveErrorCode.QUERY_SETUP_ERROR, e);
            log.error(qe);
            result.addException(qe.getBottomQueryException());
            return result;
        } finally {
            try {
                connectionFactory.returnClient(c);
            } catch (Exception e) {
                log.error("Error returning connection to connection pool", e);
            }
        }
    }
    
    /**
     * Information for a specific map reduce id
     *
     * @param id
     *            map reduce id
     * @return list of map reduce information
     */
    public MapReduceInfoResponseList findById(String id) {
        // Find out who/what called this method
        Principal p = ctx.getCallerPrincipal();
        String sid = p.getName();
        Set<Authorizations> auths = new HashSet<>();
        if (p instanceof DatawavePrincipal) {
            DatawavePrincipal dp = (DatawavePrincipal) p;
            sid = dp.getShortName();
            for (Collection<String> cbAuths : dp.getAuthorizations())
                auths.add(new Authorizations(cbAuths.toArray(new String[cbAuths.size()])));
        }
        log.trace(sid + " has authorizations " + auths);
        
        MapReduceInfoResponseList result = new MapReduceInfoResponseList();
        AccumuloClient c = null;
        try {
            Map<String,String> trackingMap = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
            c = connectionFactory.getClient(AccumuloConnectionFactory.Priority.ADMIN, trackingMap);
            tableCheck(c);
            try (Scanner scanner = ScannerHelper.createScanner(c, TABLE_NAME, auths)) {
                Range range = new Range(id);
                scanner.setRange(range);
                scanner.fetchColumnFamily(new Text(sid));
                MapReduceInfoResponse response = populateResponse(scanner);
                if (null != response)
                    result.getResults().add(response);
                return result;
            }
        } catch (IOException ioe) {
            QueryException qe = new QueryException(DatawaveErrorCode.RESPONSE_POPULATION_ERROR, ioe);
            log.error(qe);
            result.addException(qe);
            return result;
        } catch (Exception e) {
            QueryException qe = new QueryException(DatawaveErrorCode.QUERY_SETUP_ERROR, e);
            log.error(qe);
            result.addException(qe.getBottomQueryException());
            return result;
        } finally {
            try {
                connectionFactory.returnClient(c);
            } catch (Exception e) {
                log.error("Error returning connection to connection pool", e);
            }
        }
    }
    
    private MapReduceInfoResponse populateResponse(Iterable<Entry<Key,Value>> data) throws IOException {
        MapReduceInfoResponse result = null;
        String hdfs = null;
        TreeSet<JobExecution> jobs = null;
        for (Entry<Key,Value> entry : data) {
            if (null == result)
                result = new MapReduceInfoResponse();
            result.setId(entry.getKey().getRow().toString());
            String colq = entry.getKey().getColumnQualifier().toString();
            if (colq.equals(WORKING_DIRECTORY)) {
                result.setWorkingDirectory(new String(entry.getValue().get()));
            } else if (colq.equals(RESULTS_LOCATION)) {
                if (null != entry.getValue() && entry.getValue().get().length > 0) {
                    result.setResultsDirectory(new String(entry.getValue().get()));
                }
            } else if (colq.equals(PARAMS)) {
                result.setRuntimeParameters(new String(entry.getValue().get()));
            } else if (colq.equals(HDFS)) {
                result.setHdfs(new String(entry.getValue().get()));
                hdfs = new String(entry.getValue().get());
            } else if (colq.equals(JT)) {
                result.setJobTracker(new String(entry.getValue().get()));
            } else if (colq.startsWith(STATE)) {
                if (null == jobs)
                    jobs = new TreeSet<>();
                JobExecution job = new JobExecution();
                job.setMapReduceJobId(colq.substring(STATE.length() + 1));
                job.setState(new String(entry.getValue().get()));
                job.setTimestamp(entry.getKey().getTimestamp());
                jobs.add(job);
            } else if (colq.equals(NAME)) {
                result.setJobName(new String(entry.getValue().get()));
            }
        }
        if (null != jobs)
            result.setJobExecutions(new ArrayList<>(jobs));
        try {
            if (null != hdfs && !hdfs.isEmpty() && null != result.getResultsDirectory()) {
                Configuration conf = new Configuration();
                conf.set("fs.defaultFS", hdfs);
                // If we can't talk to HDFS then I want to fail fast, default is to retry 10 times.
                conf.setInt("ipc.client.connect.max.retries", 0);
                Path resultDirectoryPath = new Path(result.getResultsDirectory());
                int resultDirectoryPathLength = resultDirectoryPath.toUri().getPath().length();
                FileSystem fs = FileSystem.get(resultDirectoryPath.toUri(), conf);
                
                List<FileStatus> stats = new ArrayList<>();
                // recurse through the directory to find all files
                Queue<FileStatus> fileQueue = new LinkedList<>();
                fileQueue.add(fs.getFileStatus(resultDirectoryPath));
                while (!fileQueue.isEmpty()) {
                    FileStatus currentFileStatus = fileQueue.remove();
                    if (currentFileStatus.isFile()) {
                        
                        stats.add(currentFileStatus);
                    } else {
                        FileStatus[] dirList = fs.listStatus(currentFileStatus.getPath());
                        Collections.addAll(fileQueue, dirList);
                    }
                }
                
                // FileStatus[] stats = fs.listStatus(p);
                
                if (!stats.isEmpty()) {
                    List<ResultFile> resultFiles = new ArrayList<>();
                    for (FileStatus f : stats) {
                        if (!f.isDirectory()) {
                            ResultFile rf = new ResultFile();
                            String fullPath = f.getPath().toUri().getPath().substring(resultDirectoryPathLength + 1);
                            rf.setFileName(fullPath);
                            rf.setLength(f.getLen());
                            resultFiles.add(rf);
                        }
                    }
                    result.setResultFiles(resultFiles);
                }
            }
        } catch (IOException e) {
            log.warn("Unable to populate result files portion of response", e);
        }
        return result;
    }
    
    /**
     * Adds a new job to the history for this BulkResults id
     * 
     * @param id
     *            bulk results id
     * @param mapReduceJobId
     *            map reduce job id
     * @throws QueryException
     *             for problems with query
     */
    public void addJob(String id, String mapReduceJobId) throws QueryException {
        // Find out who/what called this method
        Principal p = ctx.getCallerPrincipal();
        String sid = p.getName();
        if (p instanceof DatawavePrincipal) {
            DatawavePrincipal dp = (DatawavePrincipal) p;
            sid = dp.getShortName();
        }
        
        AccumuloClient c = null;
        try {
            Map<String,String> trackingMap = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
            c = connectionFactory.getClient(AccumuloConnectionFactory.Priority.ADMIN, trackingMap);
            tableCheck(c);
            // Not using a MultiTableBatchWriter here because its not implemented yet
            // in Mock Accumulo.
            BatchWriterConfig bwCfg = new BatchWriterConfig().setMaxLatency(10, TimeUnit.SECONDS).setMaxMemory(10240L).setMaxWriteThreads(1);
            try (BatchWriter tableWriter = c.createBatchWriter(TABLE_NAME, bwCfg); BatchWriter indexWriter = c.createBatchWriter(INDEX_TABLE_NAME, bwCfg)) {
                Mutation m = new Mutation(id);
                m.put(sid, STATE + NULL + mapReduceJobId, new Value(MapReduceState.STARTED.toString().getBytes()));
                tableWriter.addMutation(m);
                Mutation i = new Mutation(mapReduceJobId);
                i.put(sid, id, NULL_VALUE);
                indexWriter.addMutation(i);
            }
        } catch (Exception e) {
            QueryException qe = new QueryException(DatawaveErrorCode.BULK_RESULTS_ENTRY_ERROR, e);
            log.error(qe);
            throw qe;
        } finally {
            try {
                connectionFactory.returnClient(c);
            } catch (Exception e) {
                log.error("Error closing writers", e);
            }
        }
        
    }
    
    private void tableCheck(AccumuloClient c) throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException {
        if (!c.tableOperations().exists(TABLE_NAME)) {
            c.tableOperations().create(TABLE_NAME);
            // Remove the versioning iterator
            c.tableOperations().removeProperty(TABLE_NAME, "table.iterator.majc.vers");
            c.tableOperations().removeProperty(TABLE_NAME, "table.iterator.majc.vers.opt.maxVersions");
            c.tableOperations().removeProperty(TABLE_NAME, "table.iterator.minc.vers");
            c.tableOperations().removeProperty(TABLE_NAME, "table.iterator.minc.vers.opt.maxVersions");
            c.tableOperations().removeProperty(TABLE_NAME, "table.iterator.scan.vers");
            c.tableOperations().removeProperty(TABLE_NAME, "table.iterator.scan.vers.opt.maxVersions");
        }
        if (!c.tableOperations().exists(INDEX_TABLE_NAME))
            c.tableOperations().create(INDEX_TABLE_NAME);
    }
    
    /**
     * Removes Bulk Results information and related directory in HDFS for the given job id.
     *
     * @param id
     *            bulk results id
     * @throws QueryException
     *             for problems with query
     */
    public void remove(String id) throws QueryException {
        // Find out who/what called this method
        Principal p = ctx.getCallerPrincipal();
        String sid = p.getName();
        if (p instanceof DatawavePrincipal) {
            DatawavePrincipal dp = (DatawavePrincipal) p;
            sid = dp.getShortName();
        }
        
        MapReduceInfoResponseList results = findById(id);
        if (null == results)
            throw new NotFoundQueryException(DatawaveErrorCode.NO_QUERY_OBJECT_MATCH);
        if (results.getResults().size() > 1)
            throw new NotFoundQueryException(DatawaveErrorCode.TOO_MANY_QUERY_OBJECT_MATCHES);
        else {
            MapReduceInfoResponse r = results.getResults().get(0);
            
            List<Mutation> indexEntries = new ArrayList<>();
            Mutation m = new Mutation(r.getId());
            m.putDelete(sid, WORKING_DIRECTORY);
            m.putDelete(sid, HDFS);
            m.putDelete(sid, JT);
            m.putDelete(sid, NAME);
            m.putDelete(sid, RESULTS_LOCATION);
            m.putDelete(sid, PARAMS);
            for (JobExecution job : r.getJobExecutions()) {
                m.putDelete(sid, STATE + NULL + job.getMapReduceJobId());
                Mutation i = new Mutation(job.getMapReduceJobId());
                i.putDelete(sid, r.getId());
                indexEntries.add(i);
            }
            AccumuloClient c = null;
            try {
                Map<String,String> trackingMap = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
                c = connectionFactory.getClient(AccumuloConnectionFactory.Priority.ADMIN, trackingMap);
                tableCheck(c);
                // using BatchWriter instead of MultiTableBatchWriter because Mock CB does not support
                // MultiTableBatchWriter
                BatchWriterConfig bwCfg = new BatchWriterConfig().setMaxLatency(10, TimeUnit.SECONDS).setMaxMemory(10240L).setMaxWriteThreads(1);
                try (BatchWriter tableWriter = c.createBatchWriter(TABLE_NAME, bwCfg); BatchWriter indexWriter = c.createBatchWriter(INDEX_TABLE_NAME, bwCfg)) {
                    tableWriter.addMutation(m);
                    for (Mutation i : indexEntries)
                        indexWriter.addMutation(i);
                }
            } catch (RuntimeException re) {
                throw re;
            } catch (Exception e) {
                QueryException qe = new QueryException(DatawaveErrorCode.JOB_STATE_UPDATE_ERROR, e, MessageFormat.format("job_id: {0}", id));
                log.error(qe);
                throw new QueryException(qe);
            } finally {
                try {
                    connectionFactory.returnClient(c);
                } catch (Exception e) {
                    log.error("Error creating query", e);
                }
            }
        }
    }
    
    /**
     * Class to parse a row of information from the BulkResultsJobIndex table into a BulkResultsInfoResponse
     */
    private static class MapReduceServiceJobIndex {
        
        private String user;
        private String id;
        private String mapReduceJobId;
        private String state;
        
        public String getUser() {
            return user;
        }
        
        public String getId() {
            return id;
        }
        
        public String getMapReduceJobId() {
            return mapReduceJobId;
        }
        
        public String getState() {
            return state;
        }
        
        public void setUser(String user) {
            this.user = user;
        }
        
        public void setId(String id) {
            this.id = id;
        }
        
        public void setMapReduceJobId(String mapReduceJobId) {
            this.mapReduceJobId = mapReduceJobId;
        }
        
        public void setState(String state) {
            this.state = state;
        }
        
        public static MapReduceServiceJobIndex parse(Key key, MapReduceState state) {
            MapReduceServiceJobIndex result = new MapReduceServiceJobIndex();
            result.setMapReduceJobId(key.getRow().toString());
            result.setUser(key.getColumnFamily().toString());
            result.setId(key.getColumnQualifier().toString());
            result.setState(state.name());
            return result;
        }
    }
    
}
