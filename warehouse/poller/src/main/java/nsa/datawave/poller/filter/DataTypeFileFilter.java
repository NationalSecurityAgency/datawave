package nsa.datawave.poller.filter;

import java.io.File;
import java.io.FilenameFilter;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

public class DataTypeFileFilter implements FilenameFilter, DatatypeAwareFilenameFilter, ConfigurableFilenameFilter {
    private static final Logger log = Logger.getLogger(DataTypeFileFilter.class);
    private static final String DEFAULT_DATATYPE = "default";
    private static final String FILENAME_REGEX = ".filename.regex";
    private static final String FILENAME_EXCLUDE_REGEX = ".filename.regex.exclude";
    
    private Configuration conf = null;
    private Pattern includePattern;
    private Pattern excludePattern;
    private String datatype = null;
    private boolean initialized = false;
    
    public DataTypeFileFilter() {}
    
    private void init() {
        if (null == datatype) {
            throw new IllegalArgumentException("Datatype cannot be null, call setDatatype() before using");
        }
        
        if (null == conf) {
            throw new IllegalArgumentException("Configuration not set, call setConfiguration() before using");
        }
        
        // first search for the datatype specific regex
        String nameRegex = conf.get(datatype + FILENAME_REGEX);
        if (nameRegex == null) {
            log.info("Could not load filename regular expression for " + datatype + ". Falling back to default regular expression");
            nameRegex = conf.get(DEFAULT_DATATYPE + FILENAME_REGEX);
        }
        if (nameRegex == null) {
            throw new IllegalArgumentException("Could not find a configuration for " + datatype + FILENAME_REGEX + " or " + DEFAULT_DATATYPE + FILENAME_REGEX);
        }
        
        // now get any configured exclude pattern
        String excludeRegex = conf.get(datatype + FILENAME_EXCLUDE_REGEX);
        if (excludeRegex == null) {
            excludeRegex = conf.get(DEFAULT_DATATYPE + FILENAME_EXCLUDE_REGEX);
        }
        
        includePattern = Pattern.compile(nameRegex);
        if (excludeRegex != null) {
            excludePattern = Pattern.compile(excludeRegex);
        } else {
            excludePattern = null;
        }
        initialized = true;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.io.FilenameFilter#accept(java.io.File, java.lang.String)
     */
    @Override
    public boolean accept(File dir, String name) {
        if (!initialized)
            init();
        boolean matches = includePattern.matcher(name).matches();
        boolean exclude = (excludePattern != null && excludePattern.matcher(name).matches());
        return matches && !exclude;
    }
    
    /**
     * Set the data type
     */
    @Override
    public void setDatatype(String datatype) {
        this.datatype = datatype;
    }
    
    /**
     * Add a configuration resource
     * 
     * @param resource
     */
    public void setConfiguration(Configuration conf) {
        this.conf = conf;
    }
    
}
