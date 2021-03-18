package datawave.iterators;

import datawave.query.util.*;
import datawave.util.time.DateHelper;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.*;

import java.time.LocalDate;
import java.time.Month;
import java.time.temporal.ChronoUnit;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Year;
import java.util.*;

public class IndexColumnIterator extends TypedValueCombiner<IndexedDatesValue> {
    
    static final Logger log = Logger.getLogger(IndexColumnIterator.class);
    
    private HashMap<String,IndexedDatesValue> rowIdToCompressedIndexedDatesToCQMap = new HashMap<>();
    private String ageOffDate = "19000101";
    private Calendar cal = Calendar.getInstance();
    
    public IndexColumnIterator() {}
    
    public IndexColumnIterator(IndexColumnIterator aThis, IteratorEnvironment environment) {
        super();
        setSource(aThis.getSource().deepCopy(environment));
    }
    
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        // TODO implement the function call below.
        // super.setColumns(IteratorSetting is, List< IteratorSetting.Column > columns);
        super.init(source, options, env);
        setEncoder(new IndexedDatesValueEncoder());
        if (options.get("ageOffDate") != null)
            ageOffDate = options.get("ageOffDate");
    }
    
    /**
     * Reduces a list of Values into a single Value.
     *
     * @param key
     *            The most recent version of the Key being reduced.
     *
     * @param iterator
     *            An iterator over the Values for different versions of the key.
     *
     * @return The combined Value.
     */
    @Override
    public IndexedDatesValue typedReduce(Key key, Iterator<IndexedDatesValue> iterator) {
        // The records are coming in in reverse chronological order during testing so I am
        // assuming they can come in any order in the general case.
        IndexedDatesValue aggregatedIndexedDatesValue = new IndexedDatesValue();
        IndexedDatesValue tempIndexedDatesValue;
        BitSet accumulatedDatesBitset;
        TreeSet<IndexedDatesValue> orderedStartDatesAndBitsets = new TreeSet<>();
        
        if (log.isTraceEnabled())
            log.trace("IndexColumnIterator combining dates for range for key " + key.getRow().toString(), new Exception());
        
        while (iterator.hasNext()) {
            tempIndexedDatesValue = iterator.next();
            if (tempIndexedDatesValue == null) {
                log.info("The timestamp is " + DateHelper.format(key.getTimestamp()));
                // Add the date that is not in a IndexDatesValue
            } else {
                log.info("The start date and size of the bitset is" + tempIndexedDatesValue.getStartDay() + " size of bitset: "
                                + tempIndexedDatesValue.getIndexedDatesSet().size());
                orderedStartDatesAndBitsets.add(tempIndexedDatesValue);
            }
        }
        
        YearMonthDay firstStartDay, lastStartDay;
        firstStartDay = orderedStartDatesAndBitsets.first().getStartDay();
        lastStartDay = orderedStartDatesAndBitsets.last().getStartDay();
        // Need to figure out how many days span the indexed dates so the BitSet can be sized exactly
        // Length Of Aggregated Bitset (spanOfDays) = NumberOfDays(firstStartDay, lastStartDay) + lastBitset.length
        LocalDate dateBefore = LocalDate.of(firstStartDay.getYear(), firstStartDay.getMonth(), firstStartDay.getDay());
        LocalDate dateAfter = LocalDate.of(lastStartDay.getYear(), lastStartDay.getMonth(), lastStartDay.getDay());
        long numOfDaysBetween = ChronoUnit.DAYS.between(dateBefore, dateAfter);
        
        long spanOfDays = numOfDaysBetween + Long.valueOf(orderedStartDatesAndBitsets.first().getIndexedDatesBitSet().length());
        int sizeOfBitset;
        if (spanOfDays > Integer.MAX_VALUE) {
            log.error("Date span is too long create bitset for indexes - this should not happen");
            sizeOfBitset = Integer.MAX_VALUE;
        } else {
            sizeOfBitset = (int) spanOfDays;
        }
        
        accumulatedDatesBitset = new BitSet(sizeOfBitset);
        IndexedDatesValue nextIndexedDatesValue;
        int aggregatedBitSetIndex = 0;
        YearMonthDay nextDateToContinueBitset, endDateOfLastBitset;
        
        // This iteration transfers the bits from individual bits sets to the aggregated one.
        for (Iterator<IndexedDatesValue> bitSetIterator = orderedStartDatesAndBitsets.iterator(); bitSetIterator.hasNext();) {
            
            nextIndexedDatesValue = bitSetIterator.next();
            nextDateToContinueBitset = nextIndexedDatesValue.getStartDay();
            int numDaysRepresentedInBitset = nextIndexedDatesValue.getIndexedDatesBitSet().length();
            
            for (int dayIndex = 0; dayIndex < numDaysRepresentedInBitset; dayIndex++) {
                if (nextIndexedDatesValue.getIndexedDatesBitSet().get(dayIndex)) {
                    accumulatedDatesBitset.set(dayIndex + aggregatedBitSetIndex);
                }
                nextDateToContinueBitset = YearMonthDay.nextDay(nextDateToContinueBitset.getYyyymmdd());
                aggregatedBitSetIndex++;
            }
            
            if (bitSetIterator.hasNext()) {
                nextIndexedDatesValue = bitSetIterator.next();
                numDaysRepresentedInBitset = nextIndexedDatesValue.getIndexedDatesBitSet().length();
            } else {
                break;
            }
            
            // Increment the aggregatedBitSetIndex to where you need to start setting bits again in aggregated bitset
            while (nextDateToContinueBitset.compareTo(nextIndexedDatesValue.getStartDay()) < 0) {
                aggregatedBitSetIndex++;
                nextDateToContinueBitset = YearMonthDay.nextDay(nextDateToContinueBitset.getYyyymmdd());
            }
            
            for (int dayIndex = 0; dayIndex < numDaysRepresentedInBitset; dayIndex++) {
                if (nextIndexedDatesValue.getIndexedDatesBitSet().get(dayIndex)) {
                    accumulatedDatesBitset.set(dayIndex + aggregatedBitSetIndex);
                }
                nextDateToContinueBitset = YearMonthDay.nextDay(nextDateToContinueBitset.getYyyymmdd());
                aggregatedBitSetIndex++;
            }
            
        }
        
        aggregatedIndexedDatesValue.setStartDay(firstStartDay);
        aggregatedIndexedDatesValue.setIndexedDatesBitSet(accumulatedDatesBitset);
        
        log.info("The start date and size of the bitset is" + aggregatedIndexedDatesValue.getStartDay() + " size of bitset: "
                        + aggregatedIndexedDatesValue.getIndexedDatesSet().size());
        return aggregatedIndexedDatesValue;
    }
    
    public static class IndexedDatesValueEncoder implements Encoder<IndexedDatesValue> {
        
        @Override
        public byte[] encode(IndexedDatesValue indexedDatesValue) {
            return indexedDatesValue.serialize().get();
        }
        
        @Override
        public IndexedDatesValue decode(byte[] bytes) throws ValueFormatException {
            
            if (bytes == null || bytes.length == 0)
                return new IndexedDatesValue();
            else
                // TODO Just deserialize with creating a Value object
                return IndexedDatesValue.deserialize(new Value(bytes));
        }
    }
    
    @Override
    public IteratorOptions describeOptions() {
        IteratorOptions io = super.describeOptions();
        io.addNamedOption("ageOffDate", "Indexed dates before this date are not aggregated.");
        return io;
    }
    
    @Override
    public boolean validateOptions(Map<String,String> options) {
        boolean valid = super.validateOptions(options);
        if (valid) {
            try {
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
                simpleDateFormat.parse(options.get("ageOffDate"));
            } catch (Exception e) {
                valid = false;
            }
        }
        return valid;
    }
}
