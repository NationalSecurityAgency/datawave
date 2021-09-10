package datawave.ingest.config;

import datawave.ingest.mapreduce.job.TableConfigurationUtil;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;

public class TableConfigCache extends BaseHdfsFileCacheUtil {
    
    public static final String ACCUMULO_CONFIG_CACHE_PATH_PROPERTY = "accumulo.config.cache.path";
    public static final String DEFAULT_ACCUMULO_CONFIG_CACHE_PATH = "/data/accumuloConfigCache/accConfCache.txt";
    public static final String ACCUMULO_CONFIG_CACHE_ENABLE_PROPERTY = "accumulo.config.cache.enable";
    
    protected static final String DELIMITER = "\t";
    protected static Map<String,Map<String,String>> configMap = new HashMap<>();
    
    protected static final Logger log = Logger.getLogger("datawave.ingest");
    
    public TableConfigCache(Configuration conf) {
        super(conf);
    }
    
    @SuppressFBWarnings("ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
    public void setTableConfigs(Configuration conf) {
        try {
            TableConfigurationUtil tableConfig = new TableConfigurationUtil(conf);
            configMap = tableConfig.getTableConfigs();
            // configMap = tableConfig.getTableAggregatorConfigs();
        } catch (Exception e) {
            log.error("Unable to get table configurations " + e.getMessage());
        }
    }
    
    @Override
    public void update() {
        setTableConfigs(this.conf);
        super.update();
    }
    
    @Override
    public void writeCacheFile(FileSystem fs, Path tmpCacheFile) {
        try (PrintStream out = new PrintStream(new BufferedOutputStream(fs.create(tmpCacheFile)), false, "UTF-8")) {
            for (Map.Entry<String,Map<String,String>> table : configMap.entrySet()) {
                for (Map.Entry tableProp : table.getValue().entrySet()) {
                    out.println(table.getKey() + DELIMITER + tableProp.getKey() + DELIMITER + tableProp.getValue());
                }
            }
        } catch (IOException e) {
            log.error("Unable to write cache file " + tmpCacheFile, e);
        }
    }
    
    @Override
    protected void readCache(BufferedReader in, String delimiter) throws IOException {
        this.configMap = new HashMap<>();
        String line;
        String table = null;
        String propName;
        String propVal;
        Map<String,String> tempMap = new HashMap<>();
        while ((line = in.readLine()) != null) {
            String[] parts = StringUtils.split(line, delimiter);
            if (table == null || !table.equals(parts[0])) {
                table = parts[0];
                tempMap = new HashMap<>();
                this.configMap.put(table, tempMap);
            }
            if (parts.length == 3) {
                propName = parts[1];
                propVal = parts[2];
                tempMap.put(propName, propVal);
            }// else warn?
        }
        in.close();
        
    }
    
    @Override
    public void setCacheFilePath(Configuration conf) {
        this.cacheFilePath = new Path(conf.get(ACCUMULO_CONFIG_CACHE_PATH_PROPERTY, DEFAULT_ACCUMULO_CONFIG_CACHE_PATH));
        
    }
    
    public Map<String,String> getTableProperties(String tableName) throws IOException {
        if (this.configMap.isEmpty()) {
            read();
        }
        if (null == this.configMap.get(tableName) || this.configMap.get(tableName).isEmpty()) {
            log.error("No accumulo config cache for " + tableName + ".  Please generate the accumulo config cache after ensuring the table exists.");
        }
        return this.configMap.get(tableName);
        
    }
    
}
