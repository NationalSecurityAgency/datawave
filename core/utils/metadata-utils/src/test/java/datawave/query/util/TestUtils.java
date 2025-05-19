package datawave.query.util;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;

import datawave.query.model.DateFrequencyMap;
import datawave.util.time.DateHelper;

public class TestUtils {
    
    private static final String NULL_BYTE = "\0";
    
    /**
     * Write the given mutations to the specified table via the accumulo client.
     */
    public static void writeMutations(AccumuloClient client, String tableName, Collection<Mutation> mutations) {
        BatchWriterConfig config = new BatchWriterConfig();
        config.setMaxMemory(0);
        try (BatchWriter writer = client.createBatchWriter(tableName, config)) {
            writer.addMutations(mutations);
            writer.flush();
        } catch (MutationsRejectedException | TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * Return the set of dates contained within the start and end date
     * 
     * @param startDateStr
     * @param endDateStr
     * @return
     */
    public static List<String> getDatesInRange(String startDateStr, String endDateStr) {
        Date startDate = DateHelper.parse(startDateStr);
        Date endDate = DateHelper.parse(endDateStr);
        
        List<String> dates = new ArrayList<>();
        dates.add(startDateStr);
        
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(startDate);
        while (true) {
            calendar.add(Calendar.DAY_OF_MONTH, 1);
            Date date = calendar.getTime();
            if (date.before(endDate) || date.equals(endDate)) {
                dates.add(DateHelper.format(date));
            } else {
                break;
            }
        }
        
        return dates;
    }
    
    /**
     * Create and a return a {@link DateFrequencyMap} map with the specified dates and counts. The args are expected to be alternating String dates and long
     * counts. For example:
     *
     * <pre>
     * {
     *     &#64;code
     *     DateFrequencyMap map = createDateFrequencyMap("20200101", 12L, "20200102", 34L, "20200103", 55L);
     * }
     * </pre>
     *
     * will result in a map with a count of 12 for the date 01-01-2020, 34 for the date 01-02-2020, and 55 for the date 01-03-2020.
     * 
     * @param entries
     *            the entries
     * @return the date frequency map
     */
    public static DateFrequencyMap createDateFrequencyMap(Object... entries) {
        DateFrequencyMap map = new DateFrequencyMap();
        int lastEntry = entries.length - 1;
        for (int i = 0; i < lastEntry; i++) {
            String date = (String) entries[i];
            i++;
            long count = (Long) entries[i];
            map.put(date, count);
        }
        return map;
    }
    
    /**
     * Create and a return a {@link DateFrequencyMap} map with counts for the specified date ranges. The args are expected to be alternating String date ranges
     * and long counts. For example:
     *
     * <pre>
     *  {@code
     * DateFrequencyMap map = createDateFrequencyMap("20200101", "20200105", 12L, "20200106", "20200110", 34L, "20200111", "20200115" 55L);
     * }
     * </pre>
     *
     * will result in a map with a count of 12 for the dates 01-01-2020 to 01-05-2020, 34 for the dates 01-06-2020 to 01-10-2020, and 55 for the dates
     * 01-11-2020 to 01-15-2020.
     * 
     * @param entries
     *            the entries
     * @return the date frequency map
     */
    public static DateFrequencyMap createRangedDateFrequencyMap(Object... entries) {
        DateFrequencyMap map = new DateFrequencyMap();
        int lastEntry = entries.length - 1;
        for (int i = 0; i < lastEntry; i++) {
            String startDate = (String) entries[i];
            i++;
            String endDate = (String) entries[i];
            i++;
            long count = (Long) entries[i];
            List<String> dates = getDatesInRange(startDate, endDate);
            for (String date : dates) {
                map.put(date, count);
            }
        }
        return map;
    }
    
    private TestUtils() {
        throw new UnsupportedOperationException();
    }
}
