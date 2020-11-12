package datawave.ingest;

import datawave.ingest.config.TableConfigCache;
import datawave.ingest.data.config.ingest.AccumuloHelper;
import datawave.ingest.util.ConfigurationFileHelper;
import datawave.util.cli.PasswordConverter;
import org.apache.hadoop.conf.Configuration;

public class OptionsParser {
    
    // todo: we parse these same arguments across several different classes in slightly different ways with very similar flags (e.g. zookeepers, zooKeepers,
    // zoo, etc).
    // This is a first step towards standardizing these flags.
    // The intent is to expand upon this class later, but limiting to this set for now to avoid scope creep. Must.... resist...
    
    public static final String instanceFlag = "-instance";
    public static final String zookeepersFlag = "-zookeepers";
    public static final String userFlag = "-user";
    public static final String passwordFlag = "-pass";
    public static final String accCacheDirFlag = "-accCacheDir";
    public static final String configDirFlag = "-cd";
    public static final String additionalResourceFlag = "-";
    public static final String configDirSuffix = "config.xml";
    
    // todo: ingest options and more
    
    public static Configuration parseArguments(String[] args, Configuration conf) {
        String configDir = null;
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals(instanceFlag)) {
                AccumuloHelper.setInstanceName(conf, args[++i]);
            } else if (args[i].equals(zookeepersFlag)) {
                AccumuloHelper.setZooKeepers(conf, args[++i]);
            } else if (args[i].equals(userFlag)) {
                AccumuloHelper.setUsername(conf, args[++i]);
            } else if (args[i].equals(passwordFlag)) {
                AccumuloHelper.setPassword(conf, PasswordConverter.parseArg(args[++i]).getBytes());
            } else if (args[i].equals(accCacheDirFlag)) {
                conf.set(TableConfigCache.ACCUMULO_CONFIG_CACHE_PATH_PROPERTY, args[++i]);
            } else if (args[i].equals(configDirFlag)) {
                configDir = args[++i];
            } else if (!args[i].startsWith(additionalResourceFlag)) {
                conf.addResource(args[i]);
            }
        }
        if (null != configDir) {
            ConfigurationFileHelper.setConfigurationFromFiles(conf, configDir, configDirSuffix);
        }
        return conf;
    }
    
}
