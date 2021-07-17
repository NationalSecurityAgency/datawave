package datawave.ingest.util;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.SuffixFileFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * 
 * This utility will consolidate configuration files in one directory and return a new Configuration object that contains the values with or without the hadoop
 * config default values or add them to the Configuration object that is passed in
 */
public class ConfigurationFileHelper {
    private static final org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(ConfigurationFileHelper.class);
    private static final String HADOOP_CONF_SUFFIX = "-site.xml";
    
    /**
     * Create hadoop configuration from hadoop configuration directory
     * 
     * @param configDirectory
     *            Hadoop configuration directory
     * @param configSuffix
     *            Suffix to filter files to add to the configuration
     * @param loadDefaults
     *            Will load configuration with default parameters
     * @return A hadoop configuration
     */
    public static Configuration getConfigurationFromFiles(String configDirectory, String configSuffix, boolean loadDefaults) {
        Configuration conf = new Configuration(loadDefaults);
        setConfigurationFromFiles(conf, configDirectory, configSuffix);
        return conf;
    }
    
    /**
     * Add resources to hadoop configuration
     * 
     * @param conf
     *            A hadoop configuration object
     * @param configDirectory
     *            A hadoop configuration directory
     * @param configSuffix
     *            A suffix to filter results
     */
    public static void setConfigurationFromFiles(Configuration conf, String configDirectory, String configSuffix) {
        File directory = new File(configDirectory);
        Collection<File> fileList = FileUtils.listFiles(directory, new SuffixFileFilter(configSuffix), null);
        for (File configFile : fileList) {
            try {
                conf.addResource(new Path(configFile.getCanonicalPath()));
            } catch (IOException ex) {
                log.error("Could not add config file to configuration: " + configFile, ex);
            }
        }
    }
    
    /**
     * Create a configuration for each hadoop configuration directory specified
     *
     * @param confDirs
     *            A collection of hadoop configuration directories
     * @return A collection of hadoop configuration objects
     */
    public static Collection<Configuration> getHadoopConfs(Collection<String> confDirs) {
        // @formatter:off
        return confDirs
                .stream()
                .map(dir -> ConfigurationFileHelper.getConfigurationFromFiles(dir, HADOOP_CONF_SUFFIX, false))
                .collect(Collectors.toList());
        // @formatter:on
    }
}
