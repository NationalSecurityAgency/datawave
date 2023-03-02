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
    public static final String instanceFlag2 = "-i";
    public static final String zookeepersFlag = "-zookeepers";
    public static final String zookeepersFlag2 = "-zk";
    public static final String userFlag = "-user";
    public static final String userFlag2 = "-u";
    public static final String passwordFlag = "-pass";
    public static final String passwordFlag2 = "-p";
    public static final String accCacheDirFlag = "-accCacheDir";
    public static final String configDirFlag = "-cd";
    public static final String additionalResourceFlag = "-";
    public static final String configDirSuffix = "config.xml";
    
    // todo: ingest options and more
    
    public static Configuration parseArguments(String[] args, Configuration conf) {
        String configDir = null;
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals(instanceFlag) || args[i].equals(instanceFlag2)) {
                AccumuloHelper.setInstanceName(conf, args[++i]);
            } else if (args[i].equals(zookeepersFlag) || args[i].equals(zookeepersFlag2)) {
                AccumuloHelper.setZooKeepers(conf, args[++i]);
            } else if (args[i].equals(userFlag) || args[i].equals(userFlag2)) {
                AccumuloHelper.setUsername(conf, args[++i]);
            } else if (args[i].equals(passwordFlag) || args[i].equals(passwordFlag2)) {
                AccumuloHelper.setPassword(conf, PasswordConverter.parseArg(args[++i]).getBytes());
            } else if (args[i].equals(accCacheDirFlag)) {
                conf.set(TableConfigCache.ACCUMULO_CONFIG_CACHE_PATH_PROPERTY, args[++i]);
            } else if (args[i].equals(configDirFlag)) {
                configDir = args[++i];
                if (null != configDir) {
                    ConfigurationFileHelper.setConfigurationFromFiles(conf, configDir, configDirSuffix);
                }
            } else if (args[i].startsWith(additionalResourceFlag)) {
                String configName = args[i].substring(args[i].indexOf(additionalResourceFlag) + 1);
                String configValue = args[++i];
                conf.set(configName, configValue);
            }
            
            else if (!args[i].startsWith(additionalResourceFlag)) {
                conf.addResource(args[i]);
            }
        }
        
        return conf;
    }
    
}
