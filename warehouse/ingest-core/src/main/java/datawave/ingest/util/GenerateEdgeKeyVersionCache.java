package datawave.ingest.util;

import datawave.ingest.data.config.ingest.AccumuloHelper;
import datawave.ingest.mapreduce.handler.edge.EdgeKeyVersioningCache;
import datawave.ingest.time.Now;
import datawave.util.cli.PasswordConverter;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.Date;

public class GenerateEdgeKeyVersionCache {
    private static final Now now = Now.getInstance();
    
    private static void printUsageAndExit() {
        System.out.println(
                        "Usage: datawave.ingest.util.GenerateEdgeKeyVersionCache [-init -version <integer>] [-update <cache dir>] [-addDataTypeMarkers <comma delim data types>] [<username> <password> <tableName> [<instanceName> <zookeepers>]]");
        System.exit(-1);
    }
    
    public static void main(String[] args) throws Exception {
        EdgeKeyVersioningCache edgeKeyVersioningCache;
        boolean initMode = false;
        boolean updateMode = false;
        Integer keyVersion = null;
        
        String username = null;
        byte[] password = null;
        String tableName = null;
        String instanceName = null;
        String zookeepers = null;
        
        String updateDir = null;
        
        Configuration conf = new Configuration();
        
        GenericOptionsParser parser = new GenericOptionsParser(conf, args);
        
        String[] toolArgs = parser.getRemainingArgs();
        
        try {
            for (int i = 0; i < toolArgs.length; i++) {
                
                if ("-init".equalsIgnoreCase(toolArgs[i])) {
                    initMode = true;
                } else if ("-update".equalsIgnoreCase(toolArgs[i])) {
                    updateMode = true;
                    updateDir = toolArgs[++i];
                } else if ("-version".equalsIgnoreCase(toolArgs[i])) {
                    keyVersion = Integer.parseInt(toolArgs[++i]);
                } else {
                    // need at least 3 more args
                    if (i + 3 > toolArgs.length) {
                        printUsageAndExit();
                    } else {
                        username = toolArgs[i];
                        password = PasswordConverter.parseArg(toolArgs[i + 1]).getBytes();
                        tableName = toolArgs[i + 2];
                        // skip over args
                        i += 3;
                    }
                    // if we still have args
                    if (i < toolArgs.length) {
                        // then we need exactly 2 more args
                        if (i + 2 != toolArgs.length) {
                            printUsageAndExit();
                        } else {
                            instanceName = toolArgs[i];
                            zookeepers = toolArgs[i + 1];
                            // skip over args to terminate loop
                            i += 2;
                        }
                    }
                }
                
            }
        } catch (Exception e) {
            e.printStackTrace(); // Called from main()
            printUsageAndExit();
        }
        
        // Set up accumulo connection configuration
        conf.set(AccumuloHelper.USERNAME, username);
        conf.set(AccumuloHelper.INSTANCE_NAME, instanceName);
        conf.set(AccumuloHelper.PASSWORD, Base64.encodeBase64String(password));
        conf.set(AccumuloHelper.ZOOKEEPERS, zookeepers);
        
        conf.set(EdgeKeyVersioningCache.METADATA_TABLE_NAME, tableName);
        
        if (updateDir != null) {
            conf.set(EdgeKeyVersioningCache.KEY_VERSION_CACHE_DIR, updateDir);
        }
        
        edgeKeyVersioningCache = new EdgeKeyVersioningCache(conf);
        
        if (initMode) {
            if (keyVersion == null) {
                System.out.println("Failure! Must specify key version number when running init.");
                printUsageAndExit();
            }
            Date currentTime = new Date(now.get());
            System.out.println("Creating new edge key version #" + keyVersion + "for date " + currentTime);
            edgeKeyVersioningCache.createMetadataEntry(currentTime.getTime(), keyVersion);
        }
        
        if (updateMode) {
            if (updateDir == null) {
                System.out.println("No update directory set!");
                printUsageAndExit();
            }
            
            FileSystem fs = FileSystem.get(conf);
            
            System.out.println("Updating the edge key version cache file");
            edgeKeyVersioningCache.updateCache(fs);
        }
        
        if (!initMode && !updateMode) {
            System.out.println("No arguments supplied, don't know what to do");
            printUsageAndExit();
        }
        
        System.out.println("Done");
    }
}
