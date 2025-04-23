package datawave.ingest.config;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import datawave.ingest.data.config.ingest.AccumuloHelper;

public abstract class BaseHdfsFileCacheUtil {

    protected Path cacheFilePath;
    protected final Configuration conf;
    protected AccumuloHelper accumuloHelper;

    protected String delimiter = "\t";
    private static final int MAX_RETRIES = 3;
    protected short cacheReplicas = 3;

    private static final Logger log = Logger.getLogger(BaseHdfsFileCacheUtil.class);

    public BaseHdfsFileCacheUtil(Configuration conf) {
        Validate.notNull(conf, "Configuration object passed in null");
        this.conf = conf;
        setCacheFilePath(conf);
    }

    public Path getCacheFilePath() {
        return this.cacheFilePath;
    }

    public abstract void setCacheFilePath(Configuration conf);

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public void read() throws IOException {

        int attempts = 0;
        boolean retry = true;
        while (retry && attempts <= MAX_RETRIES) {
            attempts++;

            log.info("Reading cache at " + this.cacheFilePath);
            try (BufferedReader in = new BufferedReader(new InputStreamReader(FileSystem.get(this.cacheFilePath.toUri(), conf).open(this.cacheFilePath)))) {
                readCache(in);
                retry = false;
            } catch (IOException ex) {
                if (shouldRefreshCache(this.conf)) {
                    update();
                } else if (attempts == MAX_RETRIES) {
                    throw new IOException("Unable to read cache file at " + this.cacheFilePath, ex);
                }

            }

        }

    }

    public abstract void writeCacheFile(FileSystem fs, Path tempFile) throws IOException;

    public void update() {
        FileSystem fs = null;
        Path tempFile = null;
        try {
            fs = FileSystem.get(cacheFilePath.toUri(), conf);
            tempFile = createTempFile(fs);
            writeCacheFile(fs, tempFile);
            createCacheFile(fs, tempFile);
        } catch (IOException e) {
            if (tempFile != null) {
                cleanup(fs, tempFile);
            }

            log.error("Unable to update cache file " + cacheFilePath + ". " + e.getMessage(), e);
        }

    }

    protected void initAccumuloHelper() {
        if (accumuloHelper == null) {
            accumuloHelper = new AccumuloHelper();
            accumuloHelper.setup(conf);
        }
    }

    public void createCacheFile(FileSystem fs, Path tmpCacheFile) {
        try {
            fs.delete(this.cacheFilePath, false);
            if (!fs.rename(tmpCacheFile, this.cacheFilePath)) {
                throw new IOException("Failed to rename temporary cache file");
            }
        } catch (Exception e) {
            log.warn("Unable to rename " + tmpCacheFile + " to " + this.cacheFilePath + "probably because somebody else replaced it ", e);
            cleanup(fs, tmpCacheFile);
        }
        log.info("Updated " + cacheFilePath);

    }

    protected void cleanup(FileSystem fs, Path tmpCacheFile) {
        try {
            fs.delete(tmpCacheFile, false);
        } catch (Exception e) {
            log.error("Unable to clean up " + tmpCacheFile, e);
        }
    }

    protected void readCache(BufferedReader in) throws IOException {
        String line;
        while ((line = in.readLine()) != null) {
            String[] parts = StringUtils.split(line, this.delimiter);
            if (parts.length == 2) {
                conf.set(parts[0], parts[1]);
            }
        }
        in.close();
    }

    public Path createTempFile(FileSystem fs) throws IOException {
        int count = 1;
        Path tmpCacheFile = null;
        try {
            do {
                Path parentDirectory = this.cacheFilePath.getParent();
                String fileName = this.cacheFilePath.getName() + "." + count;
                log.info("Attempting to create " + fileName + "under " + parentDirectory);
                tmpCacheFile = new Path(parentDirectory, fileName);
                count++;
            } while (!fs.createNewFile(tmpCacheFile));
        } catch (IOException ex) {
            throw new IOException("Could not create temp cache file", ex);
        }
        return tmpCacheFile;
    }

    protected boolean shouldRefreshCache(Configuration conf) {
        // most caches will be updated by external processes. we don't always want multiple clients trying to update the same file if they call update at the
        // same time (e.g. every ingest job or mapper)
        return false;
    }
}
