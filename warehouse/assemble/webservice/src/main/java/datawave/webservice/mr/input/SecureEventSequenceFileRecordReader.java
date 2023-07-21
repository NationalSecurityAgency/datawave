package datawave.webservice.mr.input;

import java.io.IOException;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.VisibilityEvaluator;
import org.apache.accumulo.core.security.VisibilityParseException;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import datawave.ingest.config.RawRecordContainerImpl;
import datawave.ingest.input.reader.event.EventSequenceFileRecordReader;

/**
 * RecordReader that returns only Events that the caller can see using the Accumulo VisibilityFilter. This class expects that
 * SecureEventSequenceFileRecordReader.authorizations property is set in the configuration with a valid set of authorizations (comma separated string)
 *
 * @param <K>
 *            - type of the file record reader
 */
public class SecureEventSequenceFileRecordReader<K> extends EventSequenceFileRecordReader<K> {

    private VisibilityEvaluator filter = null;
    public static final String AUTHS = "SecureEventSequenceFileRecordReader.authorizations";
    private static final String SPLIT = ",";

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        super.initialize(split, context);
        String auths = context.getConfiguration().get(AUTHS);
        // Ensure that the authorizations were put into the configuration
        if (null == auths || auths.isEmpty())
            throw new IOException("Authorizations not specified, expected configuration property to be set: " + AUTHS);
        // Create the VisibilityFilter with no default visibility. We expect that all Events will have a column visibility set
        Authorizations a = new Authorizations(auths.split(SPLIT));
        filter = new VisibilityEvaluator(a);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        boolean result;
        do {
            result = super.nextKeyValue();
            if (!result)
                break;
            // Need to create a Key from the Event
            RawRecordContainerImpl e = (RawRecordContainerImpl) this.getCurrentValue();
            if (null != e) {
                ColumnVisibility colviz = e.getVisibility();
                if (null != colviz && colviz.getParseTree() != null) {
                    try {
                        result = filter.evaluate(colviz);
                    } catch (VisibilityParseException e1) {
                        throw new IOException("Error evaluating column visibility: " + colviz, e1);
                    }
                } else {
                    // Event has no column visibility, should not return it.
                    result = false;
                }
            }
        } while (!result);

        return result;
    }

}
