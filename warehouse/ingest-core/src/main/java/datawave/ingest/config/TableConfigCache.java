package datawave.ingest.config;

import datawave.ingest.mapreduce.job.TableConfigurationUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Map;

public class TableConfigCache extends BaseHdfsFileCacheUtil {
    
    protected static Configuration config = new Configuration();
    
    public static final String ACCUMULO_CONFIG_CACHE_PROPERTY = "accumulo.config.cache.dir";
    public static final String DEFAULT_ACCUMULO_CONFIG_CACHE_DIR = "/data/accumuloConfigCache/accConfCache.txt";
    protected static final String DELIMITER = "\t";
    protected static Map<String,String> configMap;
    
    protected static Logger log = Logger.getLogger("datawave.ingest");
    
    public TableConfigCache(Configuration conf) {
        super(conf);
    }
    
    public void setTableConfigs(TableConfigurationUtil tableConfig, Configuration conf) {
        try {
            configMap = tableConfig.getTableAggregatorConfigs(conf);
        } catch (Exception e) {
            log.error("Unable to get table configurations " + e.getMessage());
        }
    }
    
    public void writeCacheFile(FileSystem fs, Path tmpCacheFile) {
        try (PrintStream out = new PrintStream(new BufferedOutputStream(fs.create(tmpCacheFile)))) {
            for (Map.Entry e : configMap.entrySet()) {
                out.println(e.getKey() + DELIMITER + e.getValue());
            }
        } catch (IOException e) {
            log.error("Unable to write cache file " + tmpCacheFile, e);
        }
    }
    
    @Override
    protected Path getCacheFilePath(Configuration conf) {
        this.cacheFilePath = new Path(config.get(ACCUMULO_CONFIG_CACHE_PROPERTY, DEFAULT_ACCUMULO_CONFIG_CACHE_DIR));
        return null;
    }
    
    @Override
    protected boolean shouldRefreshCache(Configuration conf) {
        // todo
        return false;
    }
}
