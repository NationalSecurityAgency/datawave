package datawave.ingest.config;

import datawave.ingest.OptionsParser;
import datawave.ingest.data.config.ingest.AccumuloHelper;
import datawave.ingest.mapreduce.job.TableConfigurationUtil;
import datawave.ingest.util.ConfigurationFileHelper;
import datawave.util.cli.PasswordConverter;
import org.apache.deltaspike.core.api.jmx.Table;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class TableConfigCacheGenerator {
    protected static Configuration config = new Configuration();
    static Path accumuloConfigCachePath;
    
    protected static Logger log = Logger.getLogger(TableConfigCache.class);
    
    public static void main(String[] args) {
        
        Configuration conf = OptionsParser.parseArguments(args, config);
        TableConfigCache cache = new TableConfigCache(conf);
        TableConfigurationUtil tableConfig = new TableConfigurationUtil(conf);
        accumuloConfigCachePath = new Path(conf.get(TableConfigCache.ACCUMULO_CONFIG_CACHE_PROPERTY, TableConfigCache.DEFAULT_ACCUMULO_CONFIG_CACHE_DIR));
        
        try {
            FileSystem fs = FileSystem.get(accumuloConfigCachePath.toUri(), conf);
            cache.setTableConfigs(tableConfig, conf);
            Path tempFile = cache.createTempFile(fs);
            cache.writeCacheFile(fs, tempFile);
            cache.createCacheFile(fs, tempFile);
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }
    
}
