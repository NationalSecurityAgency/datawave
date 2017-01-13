package nsa.datawave.metrics.web;

import nsa.datawave.metrics.config.MetricsConfig;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.deltaspike.core.api.config.ConfigProperty;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.servlet.ServletContext;
import java.io.IOException;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

@ApplicationScoped
public class CloudContext {
    private static final Logger log = Logger.getLogger(CloudContext.class);
    private Connector metrics, warehouse;
    private String ingest, analytic, purge, errors, poller, fileLatencies;
    private Configuration config;
    
    @Inject
    @ConfigProperty(name = "dw.metrics.warehouse.namenode")
    private String warehouseNamenode;
    
    public CloudContext() {
        // needed for CDI to be able to proxy this bean
    }
    
    @Inject
    public CloudContext(ServletContext ctx, @ConfigProperty(name = "dw.metrics.accumulo.userName") String metricsUsername, @ConfigProperty(
                    name = "dw.metrics.accumulo.password") String metricsPassword,
                    @ConfigProperty(name = "dw.metrics.instanceName") String metricsInstanceName,
                    @ConfigProperty(name = "dw.metrics.zookeepers") String metricsZookeepers,
                    @ConfigProperty(name = "dw.warehouse.accumulo.userName") String warehouseUsername,
                    @ConfigProperty(name = "dw.warehouse.accumulo.password") String warehousePassword,
                    @ConfigProperty(name = "dw.warehouse.instanceName") String warehouseInstanceName,
                    @ConfigProperty(name = "dw.warehouse.zookeepers") String warehouseZookeepers, @ConfigProperty(name = "dw.metrics.tables.ingest",
                                    defaultValue = MetricsConfig.DEFAULT_INGEST_TABLE) String metricsIngestTable, @ConfigProperty(
                                    name = "dw.metrics.tables.metrics", defaultValue = MetricsConfig.DEFAULT_METRICS_TABLE) String metricsAnalyticTable,
                    @ConfigProperty(name = "dw.metrics.tables.error", defaultValue = MetricsConfig.DEFAULT_ERRORS_TABLE) String metricsErrorsTable,
                    @ConfigProperty(name = "dw.metrics.tables.poller", defaultValue = MetricsConfig.DEFAULT_POLLER_TABLE) String metricsPollerTable,
                    @ConfigProperty(name = "dw.metrics.tables.filegraph", defaultValue = MetricsConfig.DEFAULT_FILE_GRAPH_TABLE) String metricsFileGraphTable,
                    @ConfigProperty(name = "dw.metrics.warehouse.hadoop.path") String warehouseHadoopPath) {
        
        try {
            metrics = new ZooKeeperInstance(ClientConfiguration.loadDefault().withInstance(metricsInstanceName).withZkHosts(metricsZookeepers)).getConnector(
                            metricsUsername, new PasswordToken(metricsPassword));
            warehouse = new ZooKeeperInstance(ClientConfiguration.loadDefault().withInstance(warehouseInstanceName).withZkHosts(warehouseZookeepers))
                            .getConnector(warehouseUsername, new PasswordToken(warehousePassword));
        } catch (AccumuloException | AccumuloSecurityException e) {
            log.warn("Unable to retrieve accumulo connection.", e);
            throw new RuntimeException(e);
        }
        
        ingest = metricsIngestTable;
        analytic = metricsAnalyticTable;
        errors = metricsErrorsTable;
        poller = metricsPollerTable;
        fileLatencies = metricsFileGraphTable;
        
        final Path hadoopPath = new Path(warehouseHadoopPath);
        log.debug("Using hadoop conf directory: " + hadoopPath);
        config = new Configuration();
        config.addResource(new Path(hadoopPath, "hdfs-site.xml"));
        config.addResource(new Path(hadoopPath, "core-site.xml"));
    }
    
    public Scanner createScanner() {
        try {
            return metrics.createScanner(analytic, Authorizations.EMPTY);
        } catch (TableNotFoundException e) {
            log.warn(e);
            return null;
        }
    }
    
    public BatchScanner createBatchScanner() {
        try {
            return metrics.createBatchScanner(analytic, Authorizations.EMPTY, 4);
        } catch (TableNotFoundException e) {
            log.warn(e);
            return null;
        }
    }
    
    public Scanner createIngestScanner() {
        try {
            return metrics.createScanner(ingest, Authorizations.EMPTY);
        } catch (TableNotFoundException e) {
            log.error(e);
        }
        return null;
    }
    
    public BatchScanner createErrorScanner() {
        try {
            return warehouse.createBatchScanner(errors, Authorizations.EMPTY, 4);
        } catch (TableNotFoundException e) {
            log.warn(e);
            return null;
        }
    }
    
    public BatchWriter createWarehouseWriter(String table) throws TableNotFoundException {
        return warehouse.createBatchWriter(table, new BatchWriterConfig().setMaxLatency(10, TimeUnit.SECONDS).setMaxMemory(256L * 1024L * 1024L)
                        .setMaxWriteThreads(11));
    }
    
    public Scanner createPollerScanner() throws TableNotFoundException {
        return metrics.createScanner(poller, Authorizations.EMPTY);
    }
    
    public Scanner createFileLatenciesScanner() throws TableNotFoundException {
        return metrics.createScanner(fileLatencies, Authorizations.EMPTY);
    }
    
    public Scanner createWarehouseScanner(String table) throws TableNotFoundException {
        return warehouse.createScanner(table, Authorizations.EMPTY);
    }
    
    public SortedMap<String,String> getWarehouseTableIdMap() {
        return Tables.getNameToIdMap(warehouse.getInstance());
    }
    
    public SortedMap<String,String> getWarehouseTableIdToNameMap() {
        return Tables.getIdToNameMap(warehouse.getInstance());
    }
    
    public Configuration getWarehouseConf() {
        return config;
    }
    
    public FileSystem getFileSystem(boolean warehouse) throws IOException {
        Configuration fsConf = config;
        
        if (warehouse) {
            if (null != warehouseNamenode) {
                fsConf = new Configuration();
                fsConf.set("fs.defaultFS", warehouseNamenode);
            }
        }
        
        return FileSystem.get(fsConf);
    }
}
