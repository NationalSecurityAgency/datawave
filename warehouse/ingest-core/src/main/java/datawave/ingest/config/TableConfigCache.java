package datawave.ingest.config;

import datawave.ingest.mapreduce.job.TableConfigurationUtil;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Map;

public class TableConfigCache extends BaseHdfsFileCacheUtil {
    
    protected static final Configuration config = new Configuration();
    
    public static final String ACCUMULO_CONFIG_CACHE_PATH_PROPERTY = "accumulo.config.cache.path";
    public static final String DEFAULT_ACCUMULO_CONFIG_CACHE_PATH = "/data/accumuloConfigCache/accConfCache.txt";
    public static final String ACCUMULO_CONFIG_CACHE_ENABLE_PROPERTY = "accumulo.config.cache.enable";
    
    protected static final String DELIMITER = "\t";
    protected static Map<String,String> configMap;
    
    protected static final Logger log = Logger.getLogger("datawave.ingest");
    
    public TableConfigCache(Configuration conf) {
        super(conf);
    }
    
    @SuppressFBWarnings("ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
    public void setTableConfigs(Configuration conf) {
        try {
            TableConfigurationUtil tableConfig = new TableConfigurationUtil(conf);
            configMap = tableConfig.getTableAggregatorConfigs();
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
            for (Map.Entry e : configMap.entrySet()) {
                out.println(e.getKey() + DELIMITER + e.getValue());
            }
        } catch (IOException e) {
            log.error("Unable to write cache file " + tmpCacheFile, e);
        }
    }
    
    @Override
    public void setCacheFilePath(Configuration conf) {
        this.cacheFilePath = new Path(config.get(ACCUMULO_CONFIG_CACHE_PATH_PROPERTY, DEFAULT_ACCUMULO_CONFIG_CACHE_PATH));
        
    }
    
}
