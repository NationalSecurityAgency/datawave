package datawave.ingest.util.cache.mode;

import datawave.common.io.FilesFinder;
import org.apache.http.util.Args;

import java.util.Collection;

/** This mode will find files to load based on the classpath and a relative path resolver */
public class ClasspathMode implements LoadJobCacheMode {
    public static final String JAVA_CLASSPATH_ENV_VAR = "CLASSPATH";
    public static final String JAVA_CLASSPATH_DELIM = ":";
    
    @Override
    public Collection<String> getFilesToLoad(ModeOptions options) {
        String baseDir = Args.notNull(options.getClasspathBaseDir(), "Classpath working directory can not be null");
        return FilesFinder.getFilesFromEnvironment(JAVA_CLASSPATH_ENV_VAR, baseDir, JAVA_CLASSPATH_DELIM);
    }
    
    @Override
    public Mode getMode() {
        return Mode.CLASSPATH;
    }
}
