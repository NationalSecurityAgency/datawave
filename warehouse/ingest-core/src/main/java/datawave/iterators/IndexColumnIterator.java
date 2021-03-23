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
    
    public IndexColumnIterator() {}
    
    public IndexColumnIterator(IndexColumnIterator aThis, IteratorEnvironment environment) {
        super();
        setSource(aThis.getSource().deepCopy(environment));
    }
    
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
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
                // AllFieldMetadataHelper.getNumField can return an null indexDatesValue and deserialize can return a temporary
                // Object that does not get fully initialized.
                // TODO fixed the logic in metadata-utils to remove the if statement below.
                if (tempIndexedDatesValue != null && tempIndexedDatesValue.getStartDay() != null)
                    orderedStartDatesAndBitsets.add(tempIndexedDatesValue);
            }
        }
        
        if (orderedStartDatesAndBitsets.size() == 0)
            return new IndexedDatesValue();
        
        YearMonthDay firstStartDay, lastStartDay;
        firstStartDay = orderedStartDatesAndBitsets.first().getStartDay();
        lastStartDay = orderedStartDatesAndBitsets.last().getStartDay();
        // Need to figure out how many days span the indexed dates so the BitSet can be sized exactly
        // Length Of Aggregated Bitset (spanOfDays) = NumberOfDays(firstStartDay, lastStartDay) + lastBitset.length
        long numOfDaysBetween;
        numOfDaysBetween = getNumOfDaysBetween(firstStartDay, lastStartDay);
        
        long spanOfDays = numOfDaysBetween + Long.valueOf(orderedStartDatesAndBitsets.first().getIndexedDatesBitSet().length()) + 2;
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
        YearMonthDay nextDateToContinueBitset = firstStartDay;
        
        // This aggregatedIndexedDatesValue is the IndexedDatesValue object that will be returned by this method
        aggregatedIndexedDatesValue.setStartDay(firstStartDay);
        aggregatedIndexedDatesValue.setIndexedDatesBitSet(accumulatedDatesBitset);
        
        // Before you go in the loop we know by definition that the first day is always indexed.
        accumulatedDatesBitset.set(0);
        aggregatedBitSetIndex++;
        boolean firstIteration = true;
        
        // This iteration transfers the bits from individual bits sets to the aggregated one.
        for (Iterator<IndexedDatesValue> bitSetIterator = orderedStartDatesAndBitsets.iterator(); bitSetIterator.hasNext();) {
            
            nextIndexedDatesValue = bitSetIterator.next();
            int numDaysRepresentedInBitset = nextIndexedDatesValue.getIndexedDatesBitSet().length();
            
            if (firstIteration) {
                firstIteration = false;
                nextDateToContinueBitset = nextIndexedDatesValue.getStartDay();
            } else {
                // Advance the aggregatedBitSetIndex by the time span between the last indexed date set
                // by the previous IndexedDatesValue and this one you are now copying the bitset from
                numOfDaysBetween = getNumOfDaysBetween(nextDateToContinueBitset, nextIndexedDatesValue.getStartDay());
                aggregatedBitSetIndex += numOfDaysBetween;
                nextDateToContinueBitset = nextIndexedDatesValue.getStartDay();
            }
            
            for (int dayIndex = 0; dayIndex < numDaysRepresentedInBitset; dayIndex++) {
                if (nextIndexedDatesValue.getIndexedDatesBitSet().get(dayIndex)) {
                    accumulatedDatesBitset.set(dayIndex + aggregatedBitSetIndex);
                }
                nextDateToContinueBitset = YearMonthDay.nextDay(nextDateToContinueBitset.getYyyymmdd());
                aggregatedBitSetIndex++;
            }
            
        }
        
        if (aggregatedBitSetIndex + 1 != sizeOfBitset)
            log.error("Aggregated bitset index should be equal to size of bitset at this point");
        
        log.info("The start date and size of the bitset is" + aggregatedIndexedDatesValue.getStartDay() + " size of bitset: "
                        + aggregatedIndexedDatesValue.getIndexedDatesSet().size());
        return aggregatedIndexedDatesValue;
    }
    
    private long getNumOfDaysBetween(YearMonthDay firstStartDay, YearMonthDay lastStartDay) {
        long numOfDaysBetween;
        LocalDate dateBefore = LocalDate.of(firstStartDay.getYear(), firstStartDay.getMonth(), firstStartDay.getDay());
        LocalDate dateAfter = LocalDate.of(lastStartDay.getYear(), lastStartDay.getMonth(), lastStartDay.getDay());
        numOfDaysBetween = ChronoUnit.DAYS.between(dateBefore, dateAfter);
        return numOfDaysBetween;
    }
    
    public static class IndexedDatesValueEncoder implements Encoder<IndexedDatesValue> {
        
        @Override
        public byte[] encode(IndexedDatesValue indexedDatesValue) {
            byte[] bytes = indexedDatesValue.serialize().get();
            Value emptyValue = new Value();
            if (bytes == null || emptyValue.get() == bytes)
                return new byte[0];
            else
                return bytes;
        }
        
        @Override
        public IndexedDatesValue decode(byte[] bytes) throws ValueFormatException {
            
            if (bytes == null || bytes.length == 0)
                return null;
            else
                // TODO Just deserialize with creating a Value object
                return IndexedDatesValue.deserialize(bytes);
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
