package datawave.ingest.util.cache.load.mode;

import datawave.common.io.FilesFinder;
import org.apache.http.util.Args;

import java.util.Collection;

/** This mode will find files to load based on the classpath and a relative path resolver */
public class ClasspathMode implements LoadJobCacheMode {
    public static final String CLASSPATH_DELIM = ":";
    
    @Override
    public Collection<String> getFilesToLoad(ModeOptions options) {
        String classpath = Args.notNull(options.getClasspath(), "Classpath can not be null");
        return FilesFinder.getFilesFromClasspath(classpath, CLASSPATH_DELIM);
    }
    
    @Override
    public Mode getMode() {
        return Mode.CLASSPATH;
    }
}
