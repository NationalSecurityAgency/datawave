package datawave.ingest.util;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

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

    /**
     *
     * @param configDirectory
     *            the config directory
     * @param configSuffix
     *            the config suffix
     * @param loadDefaults
     *            boolean flag for load defaults
     * @return a configuration
     */
    public static Configuration getConfigurationFromFiles(String configDirectory, String configSuffix, boolean loadDefaults) {
        Configuration conf = new Configuration(loadDefaults);
        setConfigurationFromFiles(conf, configDirectory, configSuffix);
        return conf;
    }

    /**
     *
     * @param conf
     *            a configuration
     * @param configDirectory
     *            the config directory
     * @param configSuffix
     *            the config suffix
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
}
