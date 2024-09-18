package datawave.core.iterators.filesystem;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * Created on 2/8/17.
 */
public class FileSystemCache {
    private static Logger log = Logger.getLogger(FileSystemCache.class);
    protected Cache<URI,FileSystem> fileSystemCache = CacheBuilder.newBuilder().expireAfterAccess(5, TimeUnit.MINUTES).concurrencyLevel(5).maximumSize(10)
                    .build();
    private Configuration conf;

    public FileSystemCache(String hdfsSiteConfigs) throws MalformedURLException {
        conf = new Configuration();
        if (hdfsSiteConfigs != null) {
            for (String url : org.apache.commons.lang.StringUtils.split(hdfsSiteConfigs, ',')) {
                conf.addResource(new URL(url));
            }
        }
    }

    private URI getSchemeAndAuthority(URI hdfsBaseURI) {
        URI defaultUri = FileSystem.getDefaultUri(conf);
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

    public void cleanup() {
        fileSystemCache.invalidateAll();
        fileSystemCache.cleanUp();
    }

    @Override
    protected void finalize() throws Throwable {
        cleanup();
    }
}
