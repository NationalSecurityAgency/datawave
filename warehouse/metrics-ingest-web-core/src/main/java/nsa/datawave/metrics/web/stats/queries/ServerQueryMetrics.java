package nsa.datawave.metrics.web.stats.queries;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;

import nsa.datawave.metrics.web.CloudContext;
import nsa.datawave.metrics.web.stats.CachedStatistic;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.util.ColumnFQ;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import javax.servlet.http.HttpServletRequest;

import java.math.BigInteger;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

/**
 * Class, which caches statistics on a configurable basis, so that they aren't re-polled
 * 
 */
public class ServerQueryMetrics extends CachedStatistic {
    
    private static final Logger log = Logger.getLogger(ServerQueryMetrics.class);
    
    protected MasterMonitorInfo masterStats;
    
    public ServerQueryMetrics(CloudContext ctx) {
        super(ctx);
    }
    
    @Override
    public void setMasterStats(MasterMonitorInfo masterStats) {
        this.masterStats = masterStats;
        
    }
    
    @Override
    public JsonElement toJson(final HttpServletRequest req) {
        
        JsonElement retVal;
        
        CachedElement element = getCachedElement();
        
        if (null != element) {
            
            if ((System.currentTimeMillis() - element.getTimeStamp()) > 1000 * 60 * 60 * 6) {
                
                retVal = buildJson();
                cacheElement(retVal);
                
            } else
                retVal = element.toJson();
            
            return retVal;
        } else {
            retVal = buildJson();
            cacheElement(retVal);
            return retVal;
        }
        
    }
    
    private JsonElement buildJson() {
        long ingestRate = 0;
        try {
            
            Scanner scanner = ctx.createWarehouseScanner(MetadataTable.NAME);
            
            ColumnFQ cfq = new ColumnFQ(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME, new Text(""));
            cfq.fetch(scanner);
            scanner.fetchColumnFamily(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME);
            
            Iterator<Entry<Key,Value>> iter = scanner.iterator();
            
            for (TabletServerStatus server : masterStats.tServerInfo) {
                new ServerQueryRate(server.getName(), server.osLoad);
            }
            
            final Map<String,String> tidToNameMap = ctx.getWarehouseTableIdToNameMap();
            
            Comparator<String> comparator = new Comparator<String>() {
                
                @Override
                public int compare(String s1, String s2) {
                    return (tidToNameMap.get(s1).compareToIgnoreCase(tidToNameMap.get(s2)));
                }
                
            };
            
            Map<String,BigInteger> tableSizeMap = new TreeMap<>(comparator);
            
            while (iter.hasNext()) {
                Entry<Key,Value> kv = iter.next();
                
                String[] byteRows = new String(kv.getValue().get()).split(",");
                
                if (byteRows.length != 2) {
                    
                    continue;
                }
                
                String id;
                String[] rowSplit = kv.getKey().getRow().toString().split(";");
                
                if (rowSplit.length != 2) {
                    int index = kv.getKey().getRow().toString().indexOf("<");
                    if (index > 0) {
                        id = kv.getKey().getRow().toString().substring(0, index);
                    } else
                        continue;
                } else {
                    id = rowSplit[0];
                }
                
                BigInteger totalSize = tableSizeMap.get(id.trim());
                
                if (null == totalSize) {
                    totalSize = new BigInteger(byteRows[0]);
                    
                } else {
                    totalSize = totalSize.add(BigInteger.valueOf(Long.valueOf(byteRows[0])));
                }
                
                tableSizeMap.put(id, totalSize);
                
            }
            
            JsonArray retVal = new JsonArray();
            JsonArray tags = new JsonArray();
            for (Entry<String,BigInteger> tableSize : tableSizeMap.entrySet()) {
                JsonArray rateObj = new JsonArray();
                rateObj.add(new JsonPrimitive(Tables.getPrintableTableNameFromId(tidToNameMap, tableSize.getKey())));
                rateObj.add(new JsonPrimitive(tableSize.getValue()));
                tags.add(rateObj);
            }
            retVal.add(tags);
            
            return tags;
            
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Error fetching stats: " + e);
            
        }
        
        JsonArray retVal = new JsonArray();
        retVal.add(new JsonPrimitive(Double.valueOf(ingestRate)));
        return retVal;
    }
    
}
