package datawave.util.flag;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

/**
 * Contains configuration parameters for Flag Maker testing. The class functions as a singleton. Any configuration or initialization errors will cause the class
 * to go into an illegal state condition.
 */
class IngestConfig {
    private static final Logger logger = LoggerFactory.getLogger(IngestConfig.class);
    final static Gson gson = new Gson();
    
    private static final AtomicReference<IngestConfig> cfg = new AtomicReference<>();
    
    // instance members
    // dir location of ingest files
    private String sourceDir;
    // HDFS ingest directory path
    private String hdfsIngestDir;
    // hdfs url
    private String hdfs;
    // duration in minutes for test
    private int duration;
    // number of worker threads to allocate
    private int workers;
    // minimum number of files to ingest for a single worker for each interval
    private int minChunks;
    // maximum number of files to ingest for a single worker for each interval
    private int maxChunks;
    // minimum interval in milliseconds between executions
    private int minInterval;
    // maximum interval in milliseconds between executions
    private int maxInterval;
    
    // transient data
    private transient List<Path> ingestPaths = new ArrayList<>();
    private transient FileSystem fs;
    
    /**
     * (U) Creates a singleton instance of the ingest load configuration.
     *
     * @param fName
     *            configuration file name
     * @return populated configuration data
     * @throws IOException
     *             error reading file
     */
    static void create(final String fName) throws IOException {
        final File f = new File(fName);
        if (!f.exists()) {
            throw new IllegalArgumentException("configuration file does not exist (" + f.getAbsolutePath() + ")");
        }
        
        final JsonParser parser = new JsonParser();
        try (final Reader rdr = new FileReader(fName)) {
            final JsonElement json = parser.parse(rdr);
            final IngestConfig val = gson.fromJson(json.toString(), IngestConfig.class);
            cfg.set(val);
            
            if (!val.validate()) {
                throw new IllegalStateException("invalid configuration: " + val.toJson());
            }
            val.getHadoopFS();
            val.loadIngestFiles();
        }
    }
    
    static IngestConfig getInstance() {
        IngestConfig val = cfg.get();
        if (null == val) {
            throw new IllegalStateException("ingest configuration has not been initialized");
        }
        return val;
    }
    
    FileSystem getHdfs() {
        return this.fs;
    }
    
    String getHdfsIngestDir() {
        return this.hdfsIngestDir;
    }
    
    int getWorkers() {
        return this.workers;
    }
    
    /**
     * Returns the duration of the test in milliseconds.
     *
     * @return duration of test in milliseconds
     */
    int getDurtion() {
        return this.duration * 1000 * 60;
    }
    
    /**
     * Returns a random interval based upon the minimum and maximum interval settings.
     * 
     * @return wait interval in milliseconds
     */
    int getRandomInterval() {
        final int diff = this.maxInterval - this.minInterval;
        final int rand = ThreadLocalRandom.current().nextInt(diff);
        return this.minInterval + rand;
    }
    
    /**
     * Returns a random list of files to ingest based upon the minimum and maximum ingest file settings.
     *
     * @return list of files
     */
    List<Path> getRandomIngestFiles() {
        final int diff = this.maxChunks - this.minChunks;
        final int rand = ThreadLocalRandom.current().nextInt(diff);
        final int num = this.minChunks + rand;
        
        final List<Path> files = new ArrayList<>();
        final int max = this.ingestPaths.size();
        for (int n = 0; n < num; n++) {
            final int idx = ThreadLocalRandom.current().nextInt(max);
            files.add(this.ingestPaths.get(idx));
        }
        
        logger.info("ingest files(" + files.size() + ")");
        return files;
    }
    
    String toJson() {
        return gson.toJson(this);
    }
    
    @Override
    public String toString() {
        return this.getClass().getSimpleName() + ": " + gson.toJson(this);
    }
    
    /**
     * Creates a {@link FileSystem} object based upon the hdfs configuration property.
     *
     * @return populated FileSystem object
     * @throws IOException
     *             error creating Hadoop FileSystem object
     */
    private void getHadoopFS() throws IOException {
        logger.info("connecting to HDFS (" + this.hdfs + ")");
        Configuration hadoopConfiguration = new Configuration();
        hadoopConfiguration.set("fs.defaultFS", this.hdfs);
        this.fs = FileSystem.get(hadoopConfiguration);
    }
    
    /**
     * Creates a list of files that are available for ingest during the test.
     */
    private void loadIngestFiles() {
        final File srcDir = new File(this.sourceDir);
        final File[] srcFiles = srcDir.listFiles();
        final Set<String> names = new HashSet<>();
        for (final File src : srcFiles) {
            names.add(src.toURI().toString());
            final Path path = new Path(src.toURI());
            this.ingestPaths.add(path);
        }
        logger.info("num: " + ingestPaths.size());
        if (ingestPaths.isEmpty()) {
            throw new IllegalStateException("no source ingest files loaded");
        }
    }
    
    /**
     * Validates the configuration data.
     *
     * @return true if all values are valid
     */
    private boolean validate() {
        // all int values must be > 0 and source directory must exist
        boolean valid = false;
        if (0 < this.workers && 0 < duration && 0 < this.minChunks && 0 < this.maxChunks && 0 < this.minInterval && 0 < this.maxInterval) {
            if (0 < this.hdfs.trim().length() && 0 < this.hdfsIngestDir.trim().length() && 0 < this.sourceDir.trim().length()) {
                final File d = new File(this.sourceDir);
                valid = d.isDirectory();
            }
        }
        
        return valid;
    }
}
