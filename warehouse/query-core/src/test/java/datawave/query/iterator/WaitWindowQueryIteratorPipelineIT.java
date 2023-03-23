package datawave.query.iterator;

import static datawave.query.iterator.QueryOptions.MAX_EVALUATION_PIPELINES;
import static datawave.query.iterator.QueryOptions.SERIAL_EVALUATION_PIPELINE;

import org.junit.Before;

/**
 * Integration test for WaitWindowObserver using the QueryIteratorIT and the PipelineIterator
 */
public class WaitWindowQueryIteratorPipelineIT extends WaitWindowQueryIteratorSerialIT {

    @Before
    public void setupPipelineIterator() {
        options.put(SERIAL_EVALUATION_PIPELINE, "false");
        options.put(MAX_EVALUATION_PIPELINES, "4");
    }
}
