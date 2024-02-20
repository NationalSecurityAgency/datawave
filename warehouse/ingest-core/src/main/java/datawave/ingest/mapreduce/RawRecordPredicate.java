package datawave.ingest.mapreduce;

import java.util.function.Predicate;

import org.apache.hadoop.conf.Configuration;

import datawave.ingest.data.RawRecordContainer;

/**
 * An implementation of this interface can be used with the EventMapper to filter out events that should not be processed. It is expected that there is an empty
 * constructor. The setConfiguration(Configuration) method will be called prior to any filtering such that the class can configure itself appropriately.
 */
public interface RawRecordPredicate extends Predicate<RawRecordContainer> {

    /**
     * This is the main method used to filter out records that should not be processed.
     *
     * @param record
     *            The raw record container under review
     * @return true if the event is ok to ingest
     */
    boolean shouldProcess(RawRecordContainer record);

    /**
     * This method will be called after configuration with the map-reduce configuration.
     *
     * @param type
     *            The datatype for which this predicate is being constructed
     * @param conf
     *            The hadoop configuration object
     */
    default void setConfiguration(String type, Configuration conf) {}

    /**
     * The counter name used for records that are dropped. Uses the simple name of the class implementation by default.
     *
     * @return The counter name
     */
    default String getCounterName() {
        return this.getClass().getSimpleName();
    }

    /**
     * The implementation of the java util Predicate method. This method should not be overridden.
     *
     * @param record
     *            The raw record container under review
     * @return true if the record should be ingested
     */
    @Override
    default boolean test(RawRecordContainer record) {
        return shouldProcess(record);
    }
}
