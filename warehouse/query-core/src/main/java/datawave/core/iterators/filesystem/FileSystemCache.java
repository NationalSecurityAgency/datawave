package datawave.core.iterators.filesystem;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.UncheckedExecutionException;

/**
 * A cache of Hadoop {@link FileSystem}s keyed by scheme and authority, backed by a {@link Configuration} built from a comma-delimited list of hdfs/core site
 * configuration URLs.
 * <p>
 * Building the {@link Configuration} (and lazily parsing its XML resources) is expensive, so callers on hot paths should obtain a shared instance via
 * {@link #getInstance(String)} rather than constructing a new one. Shared instances are never mutated after construction, so they are safe to use concurrently.
 */
public class FileSystemCache {
    private static final Cache<String,FileSystemCache> INSTANCES = CacheBuilder.newBuilder().maximumSize(32).build();

    protected Cache<URI,FileSystem> fileSystemCache = CacheBuilder.newBuilder().expireAfterAccess(5, TimeUnit.MINUTES).concurrencyLevel(5).maximumSize(10)
                    .build();
    private final Configuration conf;
    private volatile URI defaultUri;

    /**
     * Get a shared {@link FileSystemCache} for the given hdfs site configs, creating it on first use.
     *
     * @param hdfsSiteConfigs
     *            comma-delimited URLs of hadoop site configuration files, may be null
     * @return the shared instance for these configs
     * @throws MalformedURLException
     *             if any of the configured URLs is malformed
     */
    public static FileSystemCache getInstance(String hdfsSiteConfigs) throws MalformedURLException {
        // null and empty produce an identical Configuration, and cache keys may not be null
        String key = (hdfsSiteConfigs == null) ? "" : hdfsSiteConfigs;
        try {
            return INSTANCES.get(key, () -> new FileSystemCache(key));
        } catch (ExecutionException | UncheckedExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof MalformedURLException) {
                throw (MalformedURLException) cause;
            }
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw new IllegalStateException("Failed to create FileSystemCache for " + hdfsSiteConfigs, cause);
        }
    }

    /**
     * Create a new, unshared instance. Prefer {@link #getInstance(String)} unless an isolated cache is required.
     *
     * @param hdfsSiteConfigs
     *            comma-delimited URLs of hadoop site configuration files, may be null
     * @throws MalformedURLException
     *             if any of the configured URLs is malformed
     */
    public FileSystemCache(String hdfsSiteConfigs) throws MalformedURLException {
        conf = new Configuration();
        if (hdfsSiteConfigs != null) {
            for (String url : org.apache.commons.lang.StringUtils.split(hdfsSiteConfigs, ',')) {
                conf.addResource(new URL(url));
            }
        }
    }

    private URI getDefaultUri() {
        URI uri = defaultUri;
        if (uri == null) {
            uri = FileSystem.getDefaultUri(conf);
            defaultUri = uri;
        }
        return uri;
    }

    private URI getSchemeAndAuthority(URI hdfsBaseURI) {
        URI defaultUri = getDefaultUri();
        String scheme = hdfsBaseURI.getScheme();
        String authority = hdfsBaseURI.getAuthority();
        if (scheme == null && authority == null) {
            scheme = defaultUri.getScheme();
            authority = defaultUri.getAuthority();
        } else if (scheme != null && authority == null && scheme.equals(defaultUri.getScheme())) {
            authority = defaultUri.getAuthority();
        }
        try {
            return new URI(scheme, authority, "/", null, null);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Failed to create URI with only " + scheme + " and " + authority, e);
        }
    }

    public FileSystem getFileSystem(URI hdfsBaseURI) throws IOException {
        URI uri = getSchemeAndAuthority(hdfsBaseURI);
        FileSystem fs = fileSystemCache.getIfPresent(uri);
        if (fs == null) {
            fs = FileSystem.get(uri, conf);
            fileSystemCache.put(uri, fs);
        }
        return fs;
    }

    /**
     * Drop all cached {@link FileSystem} references. This does not close them. On a shared instance this affects every user, who will simply re-fetch on next
     * access.
     */
    public void cleanup() {
        fileSystemCache.invalidateAll();
        fileSystemCache.cleanUp();
    }

    @Override
    protected void finalize() throws Throwable {
        cleanup();
    }
}
