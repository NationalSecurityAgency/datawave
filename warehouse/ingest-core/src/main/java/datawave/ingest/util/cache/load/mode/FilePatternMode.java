package datawave.ingest.util.cache.load.mode;

import com.google.common.collect.Lists;
import datawave.common.io.FilesFinder;
import datawave.ingest.util.cache.load.LoadJobCacheLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;

/** This mode will find files to load based on the input path, pattern and depth specified */
public class FilePatternMode implements LoadJobCacheMode {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoadJobCacheLauncher.class);
    
    @Override
    public Collection<String> getFilesToLoad(ModeOptions options) {
        Collection<String> filesToLoad = Lists.newArrayList();
        
        String inputPath = options.getInputPath();
        String filePattern = options.getFilePattern();
        int maxDepth = options.getMaxDepth();
        
        try {
            filesToLoad = FilesFinder.getFilesFromPattern(inputPath, filePattern, maxDepth);
        } catch (IOException e) {
            LOGGER.error("Unable to get files to load from path {} and pattern {} ", inputPath, filePattern, e);
        }
        
        return filesToLoad;
    }
    
    @Override
    public Mode getMode() {
        return Mode.FILE_PATTERN;
    }
}
