package datawave.ingest.mapreduce;

import java.util.function.Predicate;

import datawave.ingest.data.RawRecordContainer;

public interface RawRecordPredicate extends Predicate<RawRecordContainer> {

    default String getCounterName() {
        return this.getClass().getSimpleName();
    }

    boolean shouldProcess(RawRecordContainer record);

    @Override
    default boolean test(RawRecordContainer record) {
        return shouldProcess(record);
    }
}
