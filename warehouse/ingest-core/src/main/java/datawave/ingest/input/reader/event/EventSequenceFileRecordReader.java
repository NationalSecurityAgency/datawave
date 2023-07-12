package datawave.ingest.input.reader.event;

import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;

import datawave.data.hash.UID;
import datawave.ingest.data.RawDataErrorNames;
import datawave.ingest.data.RawRecordContainer;

/**
 * Reads Event objects from a sequence file. Users of this class need to be aware that the Event object may not have validated and may contain errors.
 *
 *
 *
 * @param <K>
 *            the type of the record reader
 */
public class EventSequenceFileRecordReader<K> extends SequenceFileRecordReader<K,RawRecordContainer> {

    @Override
    public RawRecordContainer getCurrentValue() {
        // We are going to create a new UID for this event if it contains a digraph in the current UID
        RawRecordContainer r = super.getCurrentValue();

        if (r.getId() != null) {
            // Create a new UID if the current UID is in the old format
            String prefix = r.getId().getOptionPrefix();
            if ((null != prefix) && (prefix.length() == 2)) {
                UID newUid = UID.builder().newId(r.getRawData(), r.getTimeForUID());
                r.setId(newUid);
            }
        } else {
            // ensure we have an appropriate fatal error
            r.getErrors().add(RawDataErrorNames.UID_ERROR);
        }
        return r;

    }

}
