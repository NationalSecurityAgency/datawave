package datawave.ingest;

import org.apache.hadoop.conf.Configuration;

import datawave.ingest.config.TableConfigCache;
import datawave.ingest.data.config.ingest.AccumuloHelper;
import datawave.ingest.util.ConfigurationFileHelper;
import datawave.util.cli.PasswordConverter;

public class OptionsParser {

    // todo: we parse these same arguments across several different classes in slightly different ways with very similar flags (e.g. zookeepers, zooKeepers,
    // zoo, etc).
    // This is a first step towards standardizing these flags.
    // The intent is to expand upon this class later, but limiting to this set for now to avoid scope creep. Must.... resist...

    public static final String INSTANCE_FLAG = "-instance";
    public static final String INSTANCE_FLAG_2 = "-i";
    public static final String ZOOKEEPERS_FLAG = "-zookeepers";
    public static final String ZOOKEEPERS_FLAG_2 = "-zk";
    public static final String USER_FLAG = "-user";
    public static final String USER_FLAG_2 = "-u";
    public static final String PASSWORD_FLAG = "-pass";
    public static final String PASSWORD_FLAG_2 = "-p";
    public static final String ACC_CACHE_DIR_FLAG = "-accCacheDir";
    public static final String CONFIG_DIR_FLAG = "-cd";
    public static final String ADDITIONAL_RESOURCE_FLAG = "-";
    public static final String CONFIG_DIR_SUFFIX = "config.xml";

    // todo: ingest options and more

    public static Configuration parseArguments(String[] args, Configuration conf) {
        String configDir = null;
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals(INSTANCE_FLAG) || args[i].equals(INSTANCE_FLAG_2)) {
                AccumuloHelper.setInstanceName(conf, args[++i]);
            } else if (args[i].equals(ZOOKEEPERS_FLAG) || args[i].equals(ZOOKEEPERS_FLAG_2)) {
                AccumuloHelper.setZooKeepers(conf, args[++i]);
            } else if (args[i].equals(USER_FLAG) || args[i].equals(USER_FLAG_2)) {
                AccumuloHelper.setUsername(conf, args[++i]);
            } else if (args[i].equals(PASSWORD_FLAG) || args[i].equals(PASSWORD_FLAG_2)) {
                AccumuloHelper.setPassword(conf, PasswordConverter.parseArg(args[++i]).getBytes());
            } else if (args[i].equals(ACC_CACHE_DIR_FLAG)) {
                conf.set(TableConfigCache.ACCUMULO_CONFIG_CACHE_PATH_PROPERTY, args[++i]);
            } else if (args[i].equals(CONFIG_DIR_FLAG)) {
                configDir = args[++i];
                if (null != configDir) {
                    ConfigurationFileHelper.setConfigurationFromFiles(conf, configDir, CONFIG_DIR_SUFFIX);
                }
            } else if (args[i].startsWith(ADDITIONAL_RESOURCE_FLAG)) {
                String configName = args[i].substring(args[i].indexOf(ADDITIONAL_RESOURCE_FLAG) + 1);
                String configValue = args[++i];
                conf.set(configName, configValue);
            }

            else if (!args[i].startsWith(ADDITIONAL_RESOURCE_FLAG)) {
                conf.addResource(args[i]);
            }
        }

        return conf;
    }

}
