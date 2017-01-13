package nsa.datawave.metrics.web.stats.rfile;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import nsa.datawave.metrics.web.CloudContext;
import nsa.datawave.metrics.web.stats.CachedStatistic;
import nsa.datawave.metrics.web.stats.RFileStats.StatsBuilder;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.util.ColumnFQ;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import javax.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;

/**
 * Gets the top-n r file sizes
 * 
 */
public class RFileSizeStat extends CachedStatistic {
    
    private static final int MAX_TABLETS = 1000;
    private static final Logger log = Logger.getLogger(RFileSizeStat.class);
    
    public RFileSizeStat(CloudContext ctx) {
        super(ctx);
    }
    
    @Override
    public void setMasterStats(MasterMonitorInfo masterStats) {
        // TODO Auto-generated method stub
        
    }
    
    public void getTabletServer(final RFileSize fs) {
        Scanner scanner;
        try {
            scanner = ctx.createWarehouseScanner(MetadataTable.NAME);
            
            new ColumnFQ(MetadataSchema.TabletsSection.CurrentLocationColumnFamily.NAME, new Text("")).fetch(scanner);
            scanner.fetchColumnFamily(MetadataSchema.TabletsSection.CurrentLocationColumnFamily.NAME);
            
            String tablet = fs.getKey().getRow().toString();
            
            scanner.setRange(new Range(new Text(tablet), new Text(tablet + "~")));
            
            for (Entry<Key,Value> kv : scanner) {
                if (kv.getKey().getRow().toString().contains(";")) {
                    fs.setTserver(new String(kv.getValue().get()));
                    
                }
                
            }
            
        } catch (TableNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
    }
    
    @Override
    public JsonElement toJson(HttpServletRequest req) {
        
        String tableName = req.getParameter("tableName");
        String startNumberStr = req.getParameter("startNumber");
        String amountStr = req.getParameter("amount");
        
        JsonElement retVal;
        
        CachedElement element = getCachedElement(tableName);
        
        if (null != element) {
            if ((System.currentTimeMillis() - element.getTimeStamp()) > 1000 * 60 * 60 * 6) {
                retVal = buildJson(tableName);
                cacheElement(retVal);
                
            } else
                retVal = element.toJson();
            
        } else {
            retVal = buildJson(tableName);
            cacheElement(retVal);
        }
        
        Long startNumber;
        Long amount = 15L;
        
        try {
            startNumber = Long.valueOf(startNumberStr);
            amount = Long.valueOf(amountStr);
            
        } catch (NumberFormatException nfe) {
            startNumber = 0L; // assuming that we want to use defaults if either failed to parse
            log.debug("Failed to parse either startNumber or amount while converting to Json", nfe);
        }
        retVal = buildSet(startNumber, amount - 1, retVal);
        
        // retVal now contains the entire JSON List
        
        return retVal;
        
    }
    
    protected JsonElement buildSet(Long startNumber, Long amount, JsonElement retVal) {
        
        JsonObject newObj = new JsonObject();
        JsonElement returnValue = retVal;
        if (retVal instanceof JsonObject) {
            JsonObject jsonOb = JsonObject.class.cast(retVal);
            /*
             * retVal.add("values",values); retVal.add("ticks", ticks); retVal.add("labels",labels);
             */
            try {
                int labelsSize = jsonOb.get("labels").getAsJsonArray().size();
                newObj.add("labels", jsonOb.get("labels"));
                
                JsonArray ticksArray = jsonOb.getAsJsonArray("ticks");
                newObj.add("size", new JsonPrimitive(ticksArray.size()));
                JsonArray valuesArray = jsonOb.getAsJsonArray("values");
                
                JsonArray newTicksArray = new JsonArray();
                JsonArray newValuesArray = new JsonArray();
                
                JsonArray agesArray = jsonOb.getAsJsonArray("ages");
                JsonArray rawSizesArray = jsonOb.getAsJsonArray("rawSizes");
                JsonArray newRawSizesArray = new JsonArray();
                JsonArray newAgesArray = new JsonArray();
                
                if (valuesArray.size() != labelsSize)
                    return returnValue;
                
                if (ticksArray.size() < startNumber) {
                    return returnValue;
                } else if (ticksArray.size() < startNumber + amount) {
                    amount = ticksArray.size() - startNumber;
                }
                
                int recCount = 0;
                for (int i = 0; i < labelsSize; i++) {
                    if (valuesArray.get(i).getAsJsonArray().size() != ticksArray.size())
                        return returnValue;
                    JsonArray newArr = new JsonArray();
                    Iterator<JsonElement> valueElements = valuesArray.get(i).getAsJsonArray().iterator();
                    while (valueElements.hasNext()) {
                        if (recCount >= startNumber && recCount <= startNumber + amount) {
                            newArr.add(valueElements.next());
                            
                        } else if (recCount > startNumber + amount) {
                            break;
                        } else
                            valueElements.next();
                        recCount++;
                    }
                    recCount = 0;
                    newValuesArray.add(newArr);
                }
                
                Iterator<JsonElement> elements = ticksArray.iterator();
                
                int i = 0;
                while (elements.hasNext()) {
                    if (i >= startNumber && i <= startNumber + amount) {
                        newTicksArray.add(elements.next());
                        
                    } else if (i > startNumber + amount) {
                        break;
                    } else
                        elements.next();
                    i++;
                    
                }
                
                elements = rawSizesArray.iterator();
                
                i = 0;
                while (elements.hasNext()) {
                    if (i >= startNumber && i <= startNumber + amount) {
                        newRawSizesArray.add(elements.next());
                        
                    } else if (i > startNumber + amount) {
                        break;
                    } else
                        elements.next();
                    i++;
                    
                }
                
                elements = agesArray.iterator();
                
                i = 0;
                while (elements.hasNext()) {
                    if (i >= startNumber && i <= startNumber + amount) {
                        newAgesArray.add(elements.next());
                        
                    } else if (i > startNumber + amount) {
                        break;
                    } else
                        elements.next();
                    i++;
                    
                }
                
                newObj.add("rawSizes", newRawSizesArray);
                newObj.add("ages", newAgesArray);
                newObj.add("values", newValuesArray);
                newObj.add("ticks", newTicksArray);
                returnValue = newObj;
            } catch (NullPointerException npe) {
                log.debug(npe);
            }
            
        } else
            log.info("209");
        return returnValue;
    }
    
    protected JsonElement buildJson(String tableName) {
        
        JsonObject retVal = new JsonObject();
        
        int count = MAX_TABLETS;
        
        Scanner scanner;
        
        try {
            scanner = ctx.createWarehouseScanner(MetadataTable.NAME);
            
            Map<String,String> nameToIdMap = ctx.getWarehouseTableIdMap();
            
            String id = nameToIdMap.get(tableName);
            
            scanner.setRange(new Range(new Text(id), new Text(id + "~")));
            
            new ColumnFQ(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME, new Text("")).fetch(scanner);
            scanner.fetchColumnFamily(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME);
            
            Iterator<Entry<Key,Value>> iter = scanner.iterator();
            
            Map<String,RFileSize> tserverTabletMapping = new HashMap<>();
            
            while (iter.hasNext()) {
                Entry<Key,Value> kv = iter.next();
                
                String[] byteRows = new String(kv.getValue().get()).split(",");
                
                if (byteRows.length != 2) {
                    continue;
                }
                
                String tserverTablet = kv.getKey().getRow().toString();
                
                RFileSize sizeStats = tserverTabletMapping.get(tserverTablet);
                
                if (null == sizeStats) {
                    sizeStats = new RFileSize();
                    
                    sizeStats.setTableId(id);
                    sizeStats.setKey(kv.getKey());
                    tserverTabletMapping.put(tserverTablet, sizeStats);
                }
                sizeStats.addFile(kv.getKey().getColumnQualifier().toString());
                sizeStats.incrementFileCount();
                sizeStats.incrementSize(Long.valueOf(byteRows[0]));
                sizeStats.incrementCount(Long.valueOf(byteRows[1]));
                
            }
            TreeSet<RFileSize> sortedMap = new TreeSet<>();
            
            for (RFileSize rfSize : tserverTabletMapping.values()) {
                sortedMap.add(rfSize);
            }
            
            int i = 0;
            JsonArray values = new JsonArray();
            JsonArray labels = new JsonArray();
            JsonArray ticks = new JsonArray();
            JsonArray sizes = new JsonArray();
            JsonArray aggCounts = new JsonArray();
            JsonObject key = new JsonObject();
            JsonObject label = new JsonObject();
            label.addProperty("label", "Tablet size");
            labels.add(label);
            label.remove("label");
            label.addProperty("label", "Total Key Values");
            labels.add(label);
            
            JsonArray value = new JsonArray();
            Key nameKey;
            String row;
            
            JsonArray rfileRawSizes = new JsonArray();
            JsonArray ages = new JsonArray();
            
            for (RFileSize rfSize : sortedMap.descendingSet()) {
                // don't join on the tablet server. that information is no
                // longer necessary
                // getTabletServer(rfSize);
                updateStats(rfSize);
                
                nameKey = rfSize.getKey();
                row = nameKey.getRow().toString();
                if (row.contains(";")) {
                    row = row.substring(row.indexOf(";") + 1, row.length());
                } else
                    row = "default_tablet";
                ticks.add(new JsonPrimitive(row));
                // incrementalVal.add("value",new JsonPrimitive(
                // rfSize.getSize()) );
                sizes.add(new JsonPrimitive((rfSize.getSize())));
                aggCounts.add(new JsonPrimitive((rfSize.getAggregateRowCount())));
                
                rfileRawSizes.add(new JsonPrimitive(rfSize.getRawSize()));
                
                Collection<Long> ageCollection = rfSize.getAges();
                
                if (ageCollection.size() > 0) {
                    Long min = Collections.min(ageCollection);
                    Long max = Collections.max(ageCollection);
                    
                    ages.add(new JsonPrimitive(new Date(min).toString() + " - " + new Date(max).toString()));
                } else
                    ages.add(new JsonPrimitive("no data"));
                if (i == count) {
                    break;
                }
                i++;
                
            }
            values.add(sizes);
            values.add(aggCounts);
            key.add("values", value);
            // values.add(value);
            retVal.add("values", values);
            retVal.add("ticks", ticks);
            retVal.add("rawSizes", rfileRawSizes);
            retVal.add("ages", ages);
            retVal.add("labels", labels);
            
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return retVal;
    }
    
    private void updateStats(RFileSize rfSize) throws Exception {
        long rollingCompressedSize = 0;
        long rollingRawSize = 0;
        
        FileSystem fs = FileSystem.get(ctx.getWarehouseConf());
        
        Path p;
        Collection<String> filesToRemove = new ArrayList<>();
        for (String file : rfSize.getFiles()) {
            
            try {
                p = new Path("/accumulo/tables/" + rfSize.getTableId() + file);
                
                FSDataInputStream s = fs.open(p);
                StatsBuilder rd = new StatsBuilder(s, fs.getFileStatus(p).getLen());
                rollingCompressedSize += rd.getRFileIndexCompressedSize();
                rollingRawSize += rd.getRFileIndexRawSize();
                rfSize.setAge(file, fs.getFileStatus(p).getModificationTime());
                s.close();
            } catch (Exception e) {
                filesToRemove.add(file);
                // rfSize.removeFile( file );
            }
        }
        
        for (String fileStr : filesToRemove) {
            rfSize.removeFile(fileStr);
        }
        
        rfSize.setCompressedSize(rollingCompressedSize);
        rfSize.setRawSize(rollingRawSize);
    }
    
}
