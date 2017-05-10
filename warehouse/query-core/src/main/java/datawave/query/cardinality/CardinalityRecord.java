package datawave.query.cardinality;

import com.google.common.base.Splitter;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Sets;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

public class CardinalityRecord implements Serializable {
    private static final long serialVersionUID = 1L;
    private Set<String> resultCardinalityValueFields = null;
    private HashMultimap<Integer,DateFieldValueCardinalityRecord> cardinalityMap = HashMultimap.create();
    static private Logger log = Logger.getLogger(CardinalityRecord.class);
    
    public enum DateType {
        DOCUMENT, CURRENT
    }
    
    private DateType dateType = DateType.DOCUMENT;
    
    public CardinalityRecord(Set<String> recordedFields, DateType dateType) {
        this.resultCardinalityValueFields = recordedFields;
        this.dateType = dateType;
    }
    
    public CardinalityRecord(CardinalityRecord other) {
        synchronized (cardinalityMap) {
            this.dateType = other.dateType;
            this.resultCardinalityValueFields = new HashSet<>();
            this.resultCardinalityValueFields.addAll(other.resultCardinalityValueFields);
            
            this.cardinalityMap = HashMultimap.create();
            for (Map.Entry<Integer,DateFieldValueCardinalityRecord> entry : other.cardinalityMap.entries()) {
                this.cardinalityMap.put(entry.getKey(), new DateFieldValueCardinalityRecord(entry.getValue()));
            }
        }
    }
    
    public void addEntry(Map<String,List<String>> valueMap, String eventId, String dataType, Date dataDate) {
        
        try {
            long ts = 0;
            if (dateType.equals(DateType.DOCUMENT)) {
                ts = dataDate.getTime();
            } else {
                ts = System.currentTimeMillis();
            }
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
            String date = sdf.format(new Date(ts));
            addEntry(valueMap, eventId, dataType, date);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
    
    public void addEntry(Map<String,List<String>> valueMap, String eventId, String dataType, String date) {
        
        try {
            for (String field : this.resultCardinalityValueFields) {
                List<String> values = assembleValues(field, valueMap);
                for (String value : values) {
                    String modifiedField = field.replaceAll("\\|", "\0");
                    addResult(date, modifiedField, value, dataType, eventId);
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
    
    protected List<String> assembleValues(String field, Map<String,List<String>> valueMap) {
        
        List<String> values = new ArrayList<>();
        List<List<String>> valueLists = new ArrayList<>();
        Iterable<String> fieldSplit = Splitter.on("|").split(field);
        
        int numSplits = 0;
        for (String s : fieldSplit) {
            
            if (valueMap.containsKey(s)) {
                valueLists.add(valueMap.get(s));
            } else {
                valueLists.add(new ArrayList<String>());
                if (log.isTraceEnabled()) {
                    log.trace("Cardinalty field " + s + " of configured field " + field + " not found");
                }
            }
            numSplits++;
        }
        
        if (numSplits == 1) {
            values.addAll(valueLists.get(0));
        } else if (numSplits == 2) {
            List<String> list1 = valueLists.get(0);
            List<String> list2 = valueLists.get(1);
            if (!list1.isEmpty() && !list2.isEmpty()) {
                for (String s1 : list1) {
                    for (String s2 : list2) {
                        values.add(s1 + "\0" + s2);
                    }
                }
            } else {
                if (log.isTraceEnabled()) {
                    log.trace("did not find values for all fields in " + field);
                }
            }
        }
        
        return values;
    }
    
    protected void addRecord(DateFieldValueCardinalityRecord record) {
        
        String date = record.getEventDate();
        String fieldName = record.getFieldName();
        String fieldValue = record.getFieldValue();
        String dataType = record.getDataType();
        
        DateFieldValueCardinalityRecord fvc = getDateFieldValueCardinalityRecord(date, fieldName, fieldValue, dataType);
        if (fvc == null) {
            fvc = new DateFieldValueCardinalityRecord(date, fieldName, fieldValue, dataType);
            synchronized (cardinalityMap) {
                cardinalityMap.put(fvc.hashCode(), fvc);
            }
        }
        fvc.merge(record);
    }
    
    protected void addResult(String date, String fieldName, String fieldValue, String dataType, String eventId) {
        
        DateFieldValueCardinalityRecord fvc = getDateFieldValueCardinalityRecord(date, fieldName, fieldValue, dataType);
        if (fvc == null) {
            fvc = new DateFieldValueCardinalityRecord(date, fieldName, fieldValue, dataType);
            int hash = DateFieldValueCardinalityRecord.hash(date, fieldName, fieldValue, dataType);
            synchronized (cardinalityMap) {
                cardinalityMap.put(hash, fvc);
            }
        }
        fvc.addEventId(eventId);
    }
    
    protected DateFieldValueCardinalityRecord getDateFieldValueCardinalityRecord(String date, String fieldName, String fieldValue, String dataType) {
        
        int hash = DateFieldValueCardinalityRecord.hash(date, fieldName, fieldValue, dataType);
        Set<DateFieldValueCardinalityRecord> possibleFvcSet = null;
        synchronized (cardinalityMap) {
            possibleFvcSet = cardinalityMap.get(hash);
        }
        DateFieldValueCardinalityRecord fvc = null;
        if (possibleFvcSet != null) {
            DateFieldValueCardinalityRecord testFvc = new DateFieldValueCardinalityRecord(date, fieldName, fieldValue, dataType);
            for (DateFieldValueCardinalityRecord possFvc : possibleFvcSet) {
                if (possFvc.equals(testFvc)) {
                    fvc = possFvc;
                    break;
                }
            }
        }
        return fvc;
    }
    
    public HashMultimap<Integer,DateFieldValueCardinalityRecord> getCardinalityMap() {
        HashMultimap<Integer,DateFieldValueCardinalityRecord> newCardinalityMap = HashMultimap.create();
        synchronized (cardinalityMap) {
            for (Map.Entry<Integer,DateFieldValueCardinalityRecord> entry : cardinalityMap.entries()) {
                newCardinalityMap.put(entry.getKey(), new DateFieldValueCardinalityRecord(entry.getValue()));
            }
        }
        return newCardinalityMap;
    }
    
    public void flushToDisk(final File file) {
        
        // make a copy of the object, clear the current object, and write the copy to disk asynchronously
        CardinalityRecord newCardinalityRecord;
        synchronized (cardinalityMap) {
            newCardinalityRecord = new CardinalityRecord(this);
            this.cardinalityMap.clear();
        }
        CardinalityRecord.writeToDisk(newCardinalityRecord, file);
    }
    
    public void writeToDisk(final File file) {
        
        // make a copy of the object and write the copy to disk asynchronously
        CardinalityRecord newResulsCardinalityRecord;
        synchronized (cardinalityMap) {
            newResulsCardinalityRecord = new CardinalityRecord(this);
        }
        CardinalityRecord.writeToDisk(newResulsCardinalityRecord, file);
    }
    
    static public CardinalityRecord readFromDisk(File file) {
        
        CardinalityRecord cardinalityRecord = null;
        ObjectInputStream ois = null;
        try {
            FileInputStream fis = new FileInputStream(file);
            ois = new ObjectInputStream(fis);
            cardinalityRecord = (CardinalityRecord) ois.readObject();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeQuietly(ois);
        }
        return cardinalityRecord;
    }
    
    static public void writeToDisk(final CardinalityRecord cardinalityRecord, final File file) {
        
        if (cardinalityRecord.getCardinalityMap().size() > 0) {
            ExecutorService executor = Executors.newSingleThreadExecutor();
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    synchronized (file) {
                        ObjectOutputStream oos = null;
                        try {
                            FileOutputStream fos = new FileOutputStream(file);
                            oos = new ObjectOutputStream(fos);
                            oos.writeObject(cardinalityRecord);
                        } catch (Exception e) {
                            log.error(e.getMessage(), e);
                        } finally {
                            IOUtils.closeQuietly(oos);
                            file.notify();
                        }
                    }
                }
            });
            // schedule executor for shutdown after submitted task completes
            executor.shutdown();
        }
    }
    
    public void merge(CardinalityRecord other) {
        
        int intersectionSize = Sets.intersection(this.resultCardinalityValueFields, other.resultCardinalityValueFields).size();
        if (intersectionSize != this.resultCardinalityValueFields.size() || intersectionSize != other.resultCardinalityValueFields.size()) {
            throw new IllegalArgumentException("ResultsCardinalityRecords have different recorded fields");
        }
        
        synchronized (cardinalityMap) {
            for (DateFieldValueCardinalityRecord record : other.cardinalityMap.values()) {
                addRecord(record);
            }
        }
    }
    
    public void merge(File file) {
        
        CardinalityRecord cardinalityRecord = CardinalityRecord.readFromDisk(file);
        
        int intersectionSize = Sets.intersection(this.resultCardinalityValueFields, cardinalityRecord.resultCardinalityValueFields).size();
        if (intersectionSize != this.resultCardinalityValueFields.size() || intersectionSize != cardinalityRecord.resultCardinalityValueFields.size()) {
            throw new IllegalArgumentException("ResultsCardinalityRecords have different resultCardinalityValueFields");
        }
        
        synchronized (cardinalityMap) {
            for (DateFieldValueCardinalityRecord record : cardinalityRecord.cardinalityMap.values()) {
                addRecord(record);
            }
        }
    }
    
    public int getNumEntries() {
        return cardinalityMap.size();
    }
    
    public long getSize() {
        long size = 0;
        for (Map.Entry<Integer,DateFieldValueCardinalityRecord> entry : cardinalityMap.entries()) {
            size += entry.getValue().getSize();
        }
        return size;
    }
}
