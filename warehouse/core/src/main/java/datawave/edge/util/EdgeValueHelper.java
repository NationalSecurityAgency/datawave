package datawave.edge.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.SummingArrayCombiner.VarLongArrayEncoder;
import org.apache.log4j.Logger;

import com.google.protobuf.InvalidProtocolBufferException;

import datawave.edge.protobuf.EdgeData;
import datawave.edge.protobuf.EdgeData.EdgeValue;

/**
 * Use for STATS edge value encoding/decoding only. Otherwise use the EdgeValue class.
 *
 */
public class EdgeValueHelper {

    private Logger log = Logger.getLogger(this.getClass());

    public static final int ACTIVITY_HISTOGRAM_LENGTH = 24;
    public static final int DURATION_HISTOGRAM_LENGTH = 7;

    private EdgeValueHelper() {}

    public static List<Long> decodeActivityHistogram(Value value) {
        try {
            return decodeActivityHistogram(EdgeData.EdgeValue.parseFrom(value.get()).getHoursList());
        } catch (InvalidProtocolBufferException e) {
            // Probably an old edge value
            return decodeActivityHistogram(getVarLongList(value.get()));
        }
    }

    public static List<Long> decodeActivityHistogram(List<Long> hoursList) {
        List<Long> retLong = new ArrayList<>(ACTIVITY_HISTOGRAM_LENGTH);
        retLong.addAll(hoursList);
        fillHistogramGaps(retLong, ACTIVITY_HISTOGRAM_LENGTH);
        return retLong;
    }

    public static List<Long> decodeDurationHistogram(Value value) {
        try {
            return decodeDurationHistogram(EdgeData.EdgeValue.parseFrom(value.get()).getDurationList());
        } catch (InvalidProtocolBufferException e) {
            // Probably an old edge value
            return decodeDurationHistogram(getVarLongList(value.get()));
        }
    }

    public static List<Long> decodeDurationHistogram(List<Long> durations) {
        List<Long> retLong = new ArrayList<>(DURATION_HISTOGRAM_LENGTH);
        retLong.addAll(durations);
        // check size of return list
        fillHistogramGaps(retLong, DURATION_HISTOGRAM_LENGTH);
        return retLong;
    }

    public static void fillHistogramGaps(List<Long> retLong, int desiredLength) {
        // check size of return list
        if (retLong.size() != desiredLength) {
            for (int ii = retLong.size(); ii < desiredLength; ii++) {
                retLong.add(0l);
            }
        }
    }

    public static Long decodeLinkCount(final Value value) {
        try {
            final ExtendedHyperLogLogPlus ehllp = new ExtendedHyperLogLogPlus(value);

            return (Long.valueOf(ehllp.getCardinality()));
        } catch (IOException e) {
            return (Long.valueOf(Long.MIN_VALUE));
        }
    }

    public static List<Long> getVarLongList(byte[] bytes) {
        return new VarLongArrayEncoder().decode(bytes);
    }

    // encode stuff

    public static Value encodeActivityHistogram(List<Long> longs) {
        if (longs.size() != ACTIVITY_HISTOGRAM_LENGTH) {
            throw new IllegalArgumentException("Incorrect number of items declared in Activity Histogram (should be 24)");
        }
        EdgeValue.Builder builder = EdgeValue.newBuilder();
        builder.addAllHours(longs);
        return new Value(builder.build().toByteArray());
    }

    public static Value encodeDurationHistogram(List<Long> longs) {
        if (longs.size() != DURATION_HISTOGRAM_LENGTH) {
            throw new IllegalArgumentException("Incorrect number of items declared in Duration Histogram (should be 7)");
        }
        EdgeValue.Builder builder = EdgeValue.newBuilder();
        builder.addAllDuration(longs);
        return new Value(builder.build().toByteArray());
    }

    /**
     * Creates a long[24] array and sets element N, where N is the hour of the day (0 - 23). The element will be set to 1 if deleteRecord is false, else -1.
     *
     * @param hour
     *            - hour of the day
     * @param delete
     *            - flag to mark deleteRecord
     * @return bytes of long[24]
     */
    public static List<Long> getLongListForHour(int hour, boolean delete) {
        return initUnitList(ACTIVITY_HISTOGRAM_LENGTH, hour, delete);
    }

    public static byte[] getByteArrayForHour(int hour, boolean delete) {
        return new VarLongArrayEncoder().encode(getLongListForHour(hour, delete));
    }

    /**
     * Creates a Variable length encoded byte array of longs where the elements have the following meaning:
     * <p>
     *
     * <pre>
     * 		0   &lt; 10 sec
     * 		1   10-30 sec
     * 		2   30-60 sec
     * 		3   1-5 min
     * 		4   5-10 min
     * 		5   10-30 min
     * 		6   &gt; 30 min
     * </pre>
     *
     * <p>
     * The element at index N will be set if the duration matches the values above, otherwise it will be zero. The element will be set to 1 if deleteRecord is
     * false, else -1.
     *
     * @param elapsed
     *            - variable length of time elapsed
     * @param deleteRecord
     *            - flag to determine if the element at the index is set
     * @return bytes of long[7]
     */
    public static List<Long> getLongListForDuration(int elapsed, boolean deleteRecord) {
        /*
         * 0 < 10 sec 1 10-30 sec 2 30-60 sec 3 1-5 min 4 5-10 min 5 10-30 min 6 > 30 min
         */

        if (elapsed < 10)
            elapsed = 0;
        else if (elapsed < 30)
            elapsed = 1;
        else if (elapsed < 60)
            elapsed = 2;
        else if (elapsed < 5 * 60)
            elapsed = 3;
        else if (elapsed < 10 * 60)
            elapsed = 4;
        else if (elapsed < 30 * 60)
            elapsed = 5;
        else
            elapsed = 6;

        return initUnitList(DURATION_HISTOGRAM_LENGTH, elapsed, deleteRecord);
    }

    public static byte[] getByteArrayForDuration(int elapsed, boolean deleteRecord) {
        return new VarLongArrayEncoder().encode(getLongListForDuration(elapsed, deleteRecord));
    }

    /**
     * Initializes a List&lt;Long&gt; of length len. Sets all elements to zero, except the element at index gets the value -1 if delete is true or 1 if delete
     * is false.
     *
     * @param len
     *            - length of list
     * @param index
     *            - position of element
     * @param delete
     *            - boolean flag on the value at the index
     * @return List&lt;Long&gt; of length len.
     */
    public static List<Long> initUnitList(int len, int index, boolean delete) {
        List<Long> longList = new ArrayList<>(len);
        for (int ii = 0; ii < len; ii++) {
            if (ii == index) {
                longList.add(ii, (delete ? -1l : 1l));
            } else {
                longList.add(ii, 0l);
            }
        }
        return longList;
    }

    public static void combineHistogram(List<Long> sourceList, List<Long> combinedList) {
        if (combinedList.isEmpty()) {
            combinedList.addAll(sourceList);
        } else if (sourceList.size() != combinedList.size()) {
            throw new IllegalStateException("Decoded Values had differing lengths!");
        } else {
            for (int ii = 0; ii < sourceList.size(); ii++) {
                combinedList.set(ii, combinedList.get(ii) + sourceList.get(ii));
            }
        }
    }
}
