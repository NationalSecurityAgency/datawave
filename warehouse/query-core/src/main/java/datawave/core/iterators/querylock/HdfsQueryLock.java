package datawave.core.iterators.querylock;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import datawave.core.iterators.filesystem.FileSystemCache;
import datawave.util.StringUtils;

/**
 * Created on 2/6/17. This query lock will do nothing on start, but will create a "closed" file in the specified directories upon close. If any closed file is
 * found, the query will be considered not running.
 */
public class HdfsQueryLock implements QueryLock {
    private static Logger log = Logger.getLogger(HdfsQueryLock.class);
    private String queryId;
    private String[] hdfsBaseURIs;
    private FileSystemCache fsCache;
    private boolean privateCache = false;

    public HdfsQueryLock(String hdfsSiteConfigs, String hdfsBaseURIs, String queryId) throws MalformedURLException {
        this(new FileSystemCache(hdfsSiteConfigs), hdfsBaseURIs, queryId);
        this.privateCache = true;
    }

    public HdfsQueryLock(FileSystemCache fsCache, String hdfsBaseURIs, String queryId) throws MalformedURLException {
        this.queryId = queryId;
        this.hdfsBaseURIs = filterHdfsOnly(StringUtils.split(hdfsBaseURIs, ','));
        this.fsCache = fsCache;
    }

    private String[] filterHdfsOnly(String[] dirs) {
        String[] hdfsDirs = new String[dirs.length];
        int index = 0;
        for (String dir : dirs) {
            if (dir.startsWith("hdfs://")) {
                hdfsDirs[index++] = dir;
            }
        }
        if (index != hdfsDirs.length) {
            dirs = new String[index];
            System.arraycopy(hdfsDirs, 0, dirs, 0, index);
            hdfsDirs = dirs;
        }
        return hdfsDirs;
    }

    private Path getCacheDir(String hdfsBaseURI) {
        return new Path(hdfsBaseURI, queryId);
    }

    private Path getClosedFile(String hdfsBaseURI) {
        return new Path(getCacheDir(hdfsBaseURI), "closed");
    }

    @Override
    public void cleanup() {
        if (privateCache) {
            fsCache.cleanup();
        }
    }

    @Override
    public void startQuery() {
        // noop, the query is considered running until we create the closed files
    }

    @Override
    public void stopQuery() throws IOException {
        // create the closed files
        for (String hdfsBaseURI : hdfsBaseURIs) {
            createClosedFile(hdfsBaseURI);
        }
    }

    private void createClosedFile(String hdfsBaseURI) throws IOException {
        // get the hadoop file system and a temporary directory
        String hdfsCacheDirURI = getCacheDir(hdfsBaseURI).toString();
        final URI hdfsCacheURI;
        final FileSystem fs;
        try {
            hdfsCacheURI = new URI(hdfsCacheDirURI);
            fs = fsCache.getFileSystem(hdfsCacheURI);
            if (fs.exists(new Path(hdfsCacheURI))) {
                IOException exception = null;

                // create the cancelled file
                for (int i = 0; i < 10; i++) {
                    exception = null;
                    try {
                        if (fs.createNewFile(getClosedFile(hdfsBaseURI))) {
                            break;
                        }
                    } catch (IOException e) {
                        exception = e;
                        // try again
                    }
                }
                if (exception != null) {
                    throw new IOException("Failed to create ivarator closed file", exception);
                }
            }
        } catch (MalformedURLException e) {
            throw new IllegalStateException("Unable to load hadoop configuration", e);
        } catch (URISyntaxException e) {
            throw new IllegalStateException("Invalid hdfs cache dir URI: " + hdfsCacheDirURI, e);
        }
    }

    @Override
    public boolean isQueryRunning() {
        // if any of the closed files are found, then consider the query stopped.
        for (String hdfsBaseURI : hdfsBaseURIs) {
            // get the hadoop file system and a temporary directory
            Path closedFile = getClosedFile(hdfsBaseURI);
            try {
                FileSystem fs = fsCache.getFileSystem(closedFile.toUri());
                try {
                    if (fs.exists(closedFile)) {
                        return false;
                    }
                } catch (IOException ioe) {
                    log.error("Unable to check existance of " + closedFile, ioe);
                    // unable to check this directory....try the next
                }
            } catch (IOException e) {
                throw new IllegalStateException("Unable to create hadoop file system", e);
            }
        }
        return true;
    }

    @Override
    protected void finalize() throws Throwable {
        cleanup();
    }
}
