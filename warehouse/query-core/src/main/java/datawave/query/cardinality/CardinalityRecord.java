package datawave.query.cardinality;

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
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.google.common.base.Splitter;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Sets;

public class CardinalityRecord implements Serializable {
    private static final long serialVersionUID = 1L;
    private Set<String> resultCardinalityValueFields = null;
    private HashMultimap<Integer,DateFieldValueCardinalityRecord> cardinalityMap = HashMultimap.create();
    private static Logger log = Logger.getLogger(CardinalityRecord.class);

    public enum DateType {
        DOCUMENT, CURRENT
    }

    private DateType dateType = DateType.DOCUMENT;

    public CardinalityRecord(Set<String> recordedFields, DateType dateType) {
        this.resultCardinalityValueFields = recordedFields;
        this.dateType = dateType;
    }

    public CardinalityRecord(CardinalityRecord other) {
        synchronized (this) {
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

    public List<String> assembleValues(String field, Map<String,List<String>> valueMap) {

        List<String> values = new ArrayList<>();
        List<List<String>> valueLists = new ArrayList<>();
        Iterable<String> fieldSplit = Splitter.on("|").split(field);

        int numSplits = 0;
        for (String s : fieldSplit) {

            if (valueMap.containsKey(s)) {
                valueLists.add(valueMap.get(s));
            } else {
                valueLists.add(new ArrayList<>());
                if (log.isTraceEnabled()) {
                    log.trace("Cardinalty field " + s + " of configured field " + field + " not found");
                }
                return values;
            }
            numSplits++;
        }

        if (numSplits == 1) {
            values.addAll(valueLists.get(0));
        } else {
            List<List<String>> cartesianProduct = cartesianProduct(valueLists);
            for (List<String> l : cartesianProduct) {
                values.add(StringUtils.join(l, "\0"));
            }
        }
        return values;
    }

    protected <T> List<List<T>> cartesianProduct(List<List<T>> lists) {
        List<List<T>> resultLists = new ArrayList<>();
        if (lists.isEmpty()) {
            resultLists.add(new ArrayList<>());
            return resultLists;
        } else {
            List<T> firstList = lists.get(0);
            List<List<T>> remainingLists = cartesianProduct(lists.subList(1, lists.size()));
            for (T condition : firstList) {
                for (List<T> remainingList : remainingLists) {
                    ArrayList<T> resultList = new ArrayList<>();
                    resultList.add(condition);
                    resultList.addAll(remainingList);
                    resultLists.add(resultList);
                }
            }
        }
        return resultLists;
    }

    protected void addRecord(DateFieldValueCardinalityRecord record) {

        String date = record.getEventDate();
        String fieldName = record.getFieldName();
        String fieldValue = record.getFieldValue();
        String dataType = record.getDataType();

        DateFieldValueCardinalityRecord fvc = getDateFieldValueCardinalityRecord(date, fieldName, fieldValue, dataType);
        if (fvc == null) {
            fvc = new DateFieldValueCardinalityRecord(date, fieldName, fieldValue, dataType);
            synchronized (this) {
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
            synchronized (this) {
                cardinalityMap.put(hash, fvc);
            }
        }
        fvc.addEventId(eventId);
    }

    protected DateFieldValueCardinalityRecord getDateFieldValueCardinalityRecord(String date, String fieldName, String fieldValue, String dataType) {

        int hash = DateFieldValueCardinalityRecord.hash(date, fieldName, fieldValue, dataType);
        Set<DateFieldValueCardinalityRecord> possibleFvcSet = null;
        synchronized (this) {
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
        synchronized (this) {
            for (Map.Entry<Integer,DateFieldValueCardinalityRecord> entry : cardinalityMap.entries()) {
                newCardinalityMap.put(entry.getKey(), new DateFieldValueCardinalityRecord(entry.getValue()));
            }
        }
        return newCardinalityMap;
    }

    public void flushToDisk(final File file) {

        // make a copy of the object, clear the current object, and write the copy to disk asynchronously
        CardinalityRecord newCardinalityRecord;
        synchronized (this) {
            newCardinalityRecord = new CardinalityRecord(this);
            this.cardinalityMap.clear();
        }
        CardinalityRecord.writeToDisk(newCardinalityRecord, file);
    }

    public void writeToDisk(final File file) {

        // make a copy of the object and write the copy to disk asynchronously
        CardinalityRecord newResulsCardinalityRecord;
        synchronized (this) {
            newResulsCardinalityRecord = new CardinalityRecord(this);
        }
        CardinalityRecord.writeToDisk(newResulsCardinalityRecord, file);
    }

    public static CardinalityRecord readFromDisk(File file) {

        CardinalityRecord cardinalityRecord = null;
        try (FileInputStream fis = new FileInputStream(file); ObjectInputStream ois = new ObjectInputStream(fis)) {

            cardinalityRecord = (CardinalityRecord) ois.readObject();
        } catch (Exception e) {
            log.error(e);
        }
        return cardinalityRecord;
    }

    public static void writeToDisk(final CardinalityRecord cardinalityRecord, final File file) {

        if (!cardinalityRecord.getCardinalityMap().isEmpty()) {
            ExecutorService executor = Executors.newSingleThreadExecutor();
            executor.execute(() -> {
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

        synchronized (this) {
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

        synchronized (this) {
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
