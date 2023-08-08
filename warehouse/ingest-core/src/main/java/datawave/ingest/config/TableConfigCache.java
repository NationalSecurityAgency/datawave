package datawave.ingest.config;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class TableConfigCache extends BaseHdfsFileCacheUtil {

    public static final String ACCUMULO_CONFIG_CACHE_PATH_PROPERTY = "accumulo.config.cache.path";
    public static final String DEFAULT_ACCUMULO_CONFIG_CACHE_PATH = "/data/accumuloConfigCache/accConfCache.txt";
    public static final String ACCUMULO_CONFIG_FILE_CACHE_ENABLE_PROPERTY = "accumulo.config.cache.enable";
    public static final String ACCUMULO_CONFIG_FILE_CACHE_REPICAS_PROPERTY = "accumulo.config.cache.replicas";

    private Map<String,Map<String,String>> configMap = new HashMap<>();
    private static TableConfigCache cache;

    private static final Object lock = new Object();

    protected static final Logger log = Logger.getLogger("datawave.ingest");

    private TableConfigCache(Configuration conf) {
        super(conf);
    }

    public static TableConfigCache getCurrentCache(Configuration conf) {
        synchronized (lock) {
            if (null == cache) {
                cache = new TableConfigCache(conf);
            }
        }
        return cache;
    }

    public void setTableConfigs(Map<String,Map<String,String>> confMap) {
        configMap = confMap;
    }

    public void clear() {
        configMap = new HashMap();
        cache = null;
    }

    public boolean isInitialized() {
        return !configMap.isEmpty();
    }

    @Override
    public void writeCacheFile(FileSystem fs, Path tmpCacheFile) throws IOException {
        Map<String,Map<String,String>> tempValidationMap = configMap;

        log.info("Writing to temp file " + tmpCacheFile.getName());
        try (PrintStream out = new PrintStream(new BufferedOutputStream(fs.create(tmpCacheFile)), false, "UTF-8")) {
            for (Map.Entry<String,Map<String,String>> table : configMap.entrySet()) {
                for (Map.Entry tableProp : table.getValue().entrySet()) {
                    out.println(table.getKey() + this.delimiter + tableProp.getKey() + this.delimiter + tableProp.getValue());
                }
            }
        } catch (IOException e) {
            log.error("Unable to write cache file " + tmpCacheFile, e);
            throw e;
        }

        // validate temp file
        log.info("Validating file: " + tmpCacheFile.getName());
        try (BufferedReader in = new BufferedReader(new InputStreamReader(FileSystem.get(tmpCacheFile.toUri(), conf).open(tmpCacheFile)))) {
            readCache(in);
        } catch (IOException ex) {
            log.error("Error reading cache temp file: " + tmpCacheFile, ex);
            throw ex;
        }

        if (!configMap.equals(tempValidationMap)) {
            throw new IOException("Temporary cache file was incomplete.");
        }

        fs.setReplication(tmpCacheFile, this.cacheReplicas);

    }

    @Override
    protected void readCache(BufferedReader in) throws IOException {
        this.configMap = new HashMap<>();
        String line;
        String table = null;
        String propName;
        String propVal;
        Map<String,String> tempMap = new HashMap<>();
        while ((line = in.readLine()) != null) {
            String[] parts = StringUtils.split(line, this.delimiter);
            if (table == null || !table.equals(parts[0])) {
                table = parts[0];
                tempMap = new HashMap<>();
                this.configMap.put(table, tempMap);
            }
            if (parts.length == 3) {
                propName = parts[1];
                propVal = parts[2];
                tempMap.put(propName, propVal);
            } else {
                throw new IOException("Invalid Table Config Cache. Please verify its contents.");
            }
        }

        if (configMap.isEmpty()) {
            throw new IOException("Config cache was empty.");
        }

    }

    @Override
    public void setCacheFilePath(Configuration conf) {
        this.cacheFilePath = new Path(conf.get(ACCUMULO_CONFIG_CACHE_PATH_PROPERTY, DEFAULT_ACCUMULO_CONFIG_CACHE_PATH));
        this.cacheReplicas = (short) (conf.getInt(ACCUMULO_CONFIG_FILE_CACHE_REPICAS_PROPERTY, 3));

    }

    public Map<String,Map<String,String>> getAllTableProperties() throws IOException {
        if (this.configMap.isEmpty()) {
            read();
        }

        return this.configMap;

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
