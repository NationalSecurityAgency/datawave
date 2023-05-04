package datawave.query.metrics;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.webservice.common.connection.AccumuloConnectionFactory;
import datawave.webservice.common.connection.AccumuloConnectionFactory.Priority;

import datawave.webservice.util.EnvProvider;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import datawave.accumulo.inmemory.InMemoryInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.security.ColumnVisibility;
import datawave.common.util.ArgumentChecker;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class AccumuloRecordWriter extends RecordWriter<Text,Mutation> {
    private MultiTableBatchWriter mtbw = null;
    private HashMap<Text,BatchWriter> bws = null;
    private Text defaultTableName = null;
    private Logger log = Logger.getLogger(AccumuloRecordWriter.class);
    
    private boolean simulate = false;
    private boolean createTables = false;
    
    private long mutCount = 0;
    private long valCount = 0;
    
    private AccumuloClient client;
    private AccumuloConnectionFactory connFactory;
    private static final String PREFIX = AccumuloRecordWriter.class.getSimpleName();
    private static final String OUTPUT_INFO_HAS_BEEN_SET = PREFIX + ".configured";
    private static final String INSTANCE_HAS_BEEN_SET = PREFIX + ".instanceConfigured";
    private static final String USERNAME = PREFIX + ".username";
    private static final String PASSWORD = PREFIX + ".password";
    private static final String DEFAULT_TABLE_NAME = PREFIX + ".defaulttable";
    
    private static final String INSTANCE_NAME = PREFIX + ".instanceName";
    private static final String ZOOKEEPERS = PREFIX + ".zooKeepers";
    private static final String MOCK = ".useInMemoryInstance";
    
    private static final String CREATETABLES = PREFIX + ".createtables";
    private static final String LOGLEVEL = PREFIX + ".loglevel";
    private static final String SIMULATE = PREFIX + ".simulate";
    
    // BatchWriter options
    private static final String MAX_MUTATION_BUFFER_SIZE = PREFIX + ".maxmemory";
    private static final String MAX_LATENCY = PREFIX + ".maxlatency";
    private static final String NUM_WRITE_THREADS = PREFIX + ".writethreads";
    
    private static final long DEFAULT_MAX_MUTATION_BUFFER_SIZE = 10000000; // ~10M
    private static final int DEFAULT_MAX_LATENCY = 120000; // 1 minute
    private static final int DEFAULT_NUM_WRITE_THREADS = 4;
    
    public AccumuloRecordWriter(AccumuloConnectionFactory connectionFactory, Configuration conf) throws AccumuloException, AccumuloSecurityException,
                    IOException {
        Level l = getLogLevel(conf);
        if (l != null) {
            log.setLevel(getLogLevel(conf));
        }
        this.simulate = getSimulationMode(conf);
        this.createTables = canCreateTables(conf);
        
        if (simulate) {
            log.info("Simulating output only. No writes to tables will occur");
        }
        
        this.bws = new HashMap<>();
        
        String tname = getDefaultTableName(conf);
        this.defaultTableName = (tname == null) ? null : new Text(tname);
        
        if (!simulate) {
            try {
                if (connectionFactory == null) {
                    this.client = getClient(conf);
                } else {
                    this.connFactory = connectionFactory;
                    Map<String,String> trackingMap = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
                    this.client = connectionFactory.getClient(Priority.ADMIN, trackingMap);
                }
                mtbw = client.createMultiTableBatchWriter(new BatchWriterConfig().setMaxMemory(getMaxMutationBufferSize(conf))
                                .setMaxLatency(getMaxLatency(conf), TimeUnit.MILLISECONDS).setMaxWriteThreads(getMaxWriteThreads(conf)));
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }
    
    /**
     * Push a mutation into a table. If table is null, the defaultTable will be used. If canCreateTable is set, the table will be created if it does not exist.
     * The table name must only contain alphanumerics and underscore.
     * 
     * @param mutation
     *            a mutation
     * @param table
     *            the table
     * @throws IOException
     *             for issues with read/write
     */
    @Override
    public void write(Text table, Mutation mutation) throws IOException {
        if (table == null || table.toString().isEmpty()) {
            table = this.defaultTableName;
        }
        
        if (!simulate && table == null) {
            throw new IOException("No table or default table specified. Try simulation mode next time");
        }
        
        ++mutCount;
        valCount += mutation.size();
        printMutation(table, mutation);
        
        if (simulate) {
            return;
        }
        
        if (!bws.containsKey(table)) {
            try {
                addTable(table);
            } catch (Exception e) {
                log.error(e);
                throw new IOException(e);
            }
        }
        
        try {
            bws.get(table).addMutation(mutation);
        } catch (MutationsRejectedException e) {
            log.error("Mutation rejected with constraint violations: " + e.getConstraintViolationSummaries() + " row: " + mutation.getRow() + " updates: "
                            + mutation.getUpdates());
            throw new IOException("MutationsRejectedException - ConstraintViolations: " + e.getConstraintViolationSummaries(), e);
        }
    }
    
    public void addTable(Text tableName) throws AccumuloException, AccumuloSecurityException {
        if (simulate) {
            log.info("Simulating adding table: " + tableName);
            return;
        }
        
        log.debug("Adding table: " + tableName);
        BatchWriter bw = null;
        String table = tableName.toString();
        
        if (createTables && !client.tableOperations().exists(table)) {
            try {
                client.tableOperations().create(table);
            } catch (AccumuloSecurityException e) {
                log.error("Accumulo security violation creating " + table, e);
                throw e;
            } catch (TableExistsException e) {
                // Shouldn't happen
            }
        }
        
        try {
            bw = mtbw.getBatchWriter(table);
        } catch (TableNotFoundException e) {
            log.error("Accumulo table " + table + " doesn't exist and cannot be created.", e);
            throw new AccumuloException(e);
        }
        
        if (bw != null) {
            bws.put(tableName, bw);
        }
    }
    
    private int printMutation(Text table, Mutation m) {
        if (log.isTraceEnabled()) {
            log.trace(String.format("Table %s row key: %s", table, hexDump(m.getRow())));
            for (ColumnUpdate cu : m.getUpdates()) {
                log.trace(String.format("Table %s column: %s:%s", table, hexDump(cu.getColumnFamily()), hexDump(cu.getColumnQualifier())));
                log.trace(String.format("Table %s security: %s", table, new ColumnVisibility(cu.getColumnVisibility()).toString()));
                log.trace(String.format("Table %s value: %s", table, hexDump(cu.getValue())));
            }
        }
        return m.getUpdates().size();
    }
    
    private String hexDump(byte[] ba) {
        StringBuilder sb = new StringBuilder();
        for (byte b : ba) {
            if ((b > 0x20) && (b < 0x7e)) {
                sb.append((char) b);
            } else {
                sb.append(String.format("x%02x", b));
            }
        }
        return sb.toString();
    }
    
    @Override
    public void close(TaskAttemptContext attempt) throws IOException, InterruptedException {
        log.debug("mutations written: " + mutCount + ", values written: " + valCount);
        if (simulate) {
            return;
        }
        
        try {
            mtbw.close();
        } catch (MutationsRejectedException e) {
            if (e.getSecurityErrorCodes().size() >= 0) {
                HashSet<String> tables = new HashSet<>();
                for (TabletId tabletId : e.getSecurityErrorCodes().keySet()) {
                    tables.add(tabletId.getTable().toString());
                }
                
                log.error("Not authorized to write to tables : " + tables);
            }
            
            if (!e.getConstraintViolationSummaries().isEmpty()) {
                log.error("Constraint violations : " + e.getConstraintViolationSummaries());
            }
        } finally {
            returnConnector();
        }
    }
    
    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        returnConnector();
    }
    
    public void returnConnector() {
        if (null != this.connFactory) {
            log.debug("Non-null connection factory");
            if (null != this.client) {
                log.debug("Non-null connector to return");
                try {
                    this.connFactory.returnClient(this.client);
                } catch (Exception e) {
                    log.warn("Could not return connection to pool", e);
                }
                this.client = null;
            }
        }
    }
    
    public static void setZooKeeperInstance(Configuration conf, String instanceName, String zooKeepers) {
        if (conf.getBoolean(INSTANCE_HAS_BEEN_SET, false)) {
            throw new IllegalStateException("Instance info can only be set once per job");
        }
        conf.setBoolean(INSTANCE_HAS_BEEN_SET, true);
        
        ArgumentChecker.notNull(instanceName, zooKeepers);
        conf.set(INSTANCE_NAME, instanceName);
        conf.set(ZOOKEEPERS, zooKeepers);
    }
    
    public static void setInMemoryInstance(Configuration conf, String instanceName) {
        conf.setBoolean(INSTANCE_HAS_BEEN_SET, true);
        conf.setBoolean(MOCK, true);
        conf.set(INSTANCE_NAME, instanceName);
    }
    
    public static void setMaxMutationBufferSize(Configuration conf, long numberOfBytes) {
        conf.setLong(MAX_MUTATION_BUFFER_SIZE, numberOfBytes);
    }
    
    public static void setMaxLatency(Configuration conf, int numberOfMilliseconds) {
        conf.setInt(MAX_LATENCY, numberOfMilliseconds);
    }
    
    public static void setMaxWriteThreads(Configuration conf, int numberOfThreads) {
        conf.setInt(NUM_WRITE_THREADS, numberOfThreads);
    }
    
    public static void setLogLevel(Configuration conf, Level level) {
        ArgumentChecker.notNull(level);
        conf.setInt(LOGLEVEL, level.toInt());
    }
    
    public static void setSimulationMode(Configuration conf) {
        conf.setBoolean(SIMULATE, true);
    }
    
    protected static String getUsername(Configuration conf) {
        return conf.get(USERNAME);
    }
    
    /**
     * WARNING: The password is stored in the Configuration and shared with all MapReduce tasks; It is BASE64 encoded to provide a charset safe conversion to a
     * string, and is not intended to be secure.
     * 
     * @param conf
     *            a configuration
     * @return the password
     */
    protected static byte[] getPassword(Configuration conf) {
        byte[] bytes = Base64.decodeBase64(conf.get(PASSWORD, "").getBytes());
        String pw = new String(bytes);
        pw = EnvProvider.resolve(pw);
        return pw.getBytes(StandardCharsets.UTF_8);
    }
    
    protected static boolean canCreateTables(Configuration conf) {
        return conf.getBoolean(CREATETABLES, false);
    }
    
    protected static String getDefaultTableName(Configuration conf) {
        return conf.get(DEFAULT_TABLE_NAME);
    }
    
    protected static AccumuloClient getClient(Configuration conf) throws AccumuloSecurityException, AccumuloException {
        if (conf.getBoolean(MOCK, false)) {
            InMemoryAccumuloClient client = new InMemoryAccumuloClient(getUsername(conf), new InMemoryInstance(conf.get(INSTANCE_NAME)));
            client.securityOperations().changeLocalUserPassword(client.whoami(), new PasswordToken(getPassword(conf)));
            return client;
        } else {
            return Accumulo.newClient().to(conf.get(INSTANCE_NAME), conf.get(ZOOKEEPERS)).as(getUsername(conf), new PasswordToken(getPassword(conf))).build();
        }
    }
    
    protected static long getMaxMutationBufferSize(Configuration conf) {
        return conf.getLong(MAX_MUTATION_BUFFER_SIZE, DEFAULT_MAX_MUTATION_BUFFER_SIZE);
    }
    
    protected static int getMaxLatency(Configuration conf) {
        return conf.getInt(MAX_LATENCY, DEFAULT_MAX_LATENCY);
    }
    
    protected static int getMaxWriteThreads(Configuration conf) {
        return conf.getInt(NUM_WRITE_THREADS, DEFAULT_NUM_WRITE_THREADS);
    }
    
    protected static Level getLogLevel(Configuration conf) {
        if (conf.get(LOGLEVEL) != null) {
            return Level.toLevel(conf.getInt(LOGLEVEL, Level.INFO.toInt()));
        }
        return null;
    }
    
    protected static boolean getSimulationMode(Configuration conf) {
        return conf.getBoolean(SIMULATE, false);
    }
    
    public void flush() throws Exception {
        this.mtbw.flush();
    }
}
