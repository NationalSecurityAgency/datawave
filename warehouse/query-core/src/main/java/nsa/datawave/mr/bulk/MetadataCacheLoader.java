package nsa.datawave.mr.bulk;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import nsa.datawave.query.util.Tuple2;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.google.common.cache.CacheLoader;
import com.google.common.collect.Sets;

public class MetadataCacheLoader extends CacheLoader<Range,Tuple2<String,Set<String>>> {
    
    private static final Logger log = Logger.getLogger(MetadataCacheLoader.class);
    protected Connector conn = null;
    protected String defaultBasePath;
    private int retries;
    
    private static final String HDFS_BASE = "hdfs://";
    
    public MetadataCacheLoader(Connector connector, String defaultBasePath, int retries) {
        conn = connector;
        this.defaultBasePath = defaultBasePath;
        this.retries = retries;
    }
    
    @Override
    public Tuple2<String,Set<String>> load(Range inputKey) throws Exception {
        
        final Range metadataRange = inputKey;
        Scanner scanner = conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
        MetadataSchema.TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.fetch(scanner);
        scanner.fetchColumnFamily(MetadataSchema.TabletsSection.LastLocationColumnFamily.NAME);
        scanner.fetchColumnFamily(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME);
        scanner.fetchColumnFamily(MetadataSchema.TabletsSection.CurrentLocationColumnFamily.NAME);
        scanner.fetchColumnFamily(MetadataSchema.TabletsSection.FutureLocationColumnFamily.NAME);
        
        scanner.setRange(metadataRange);
        
        RowIterator rowIter = new RowIterator(scanner);
        
        final String metadataString = metadataRange.getStartKey().getRow().toString().intern();
        final String tableId = metadataString.substring(0, metadataString.indexOf(";"));
        String baseLocation = defaultBasePath + MultiRfileInputformat.tableStr + tableId + Path.SEPARATOR;
        int attempts = 0;
        long sleepTime = TimeUnit.SECONDS.toNanos(1);
        try {
            do {
                if (rowIter.hasNext() == false) {
                    break;
                }
                while (rowIter.hasNext()) {
                    
                    Iterator<Entry<Key,Value>> row = rowIter.next();
                    String location = "";
                    Set<String> fileLocations = Sets.newHashSet();
                    
                    while (row.hasNext()) {
                        Entry<Key,Value> entry = row.next();
                        Key key = entry.getKey();
                        
                        if (key.getColumnFamily().equals(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME)) {
                            String fileLocation = entry.getKey().getColumnQualifier().toString();
                            if (!fileLocation.contains(HDFS_BASE))
                                fileLocation = baseLocation.concat(entry.getKey().getColumnQualifier().toString());
                            fileLocations.add(fileLocation);
                        }
                        
                        if (key.getColumnFamily().equals(MetadataSchema.TabletsSection.CurrentLocationColumnFamily.NAME)
                                        || key.getColumnFamily().equals(MetadataSchema.TabletsSection.FutureLocationColumnFamily.NAME)) {
                            location = entry.getValue().toString();
                        }
                        
                    }
                    
                    if (location.isEmpty() || fileLocations.isEmpty()) {
                        attempts++;
                        LockSupport.parkNanos(sleepTime);
                        if (Thread.interrupted()) {
                            throw new InterruptedException("Interrupted while parking");
                        }
                    }
                    // return the new location;
                    return new Tuple2<String,Set<String>>(location, fileLocations);
                }
            } while (attempts < retries);
        } finally {
            scanner.close();
        }
        return new Tuple2<String,Set<String>>(null, null);
    }
    
}
