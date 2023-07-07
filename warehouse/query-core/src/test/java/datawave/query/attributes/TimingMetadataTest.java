package datawave.query.attributes;

import org.junit.Ignore;
import org.junit.Test;

public class TimingMetadataTest extends AttributeTest {

    @Ignore
    @Test
    public void testSerializationOfToKeepFlag() {
        TimingMetadata metadata = createTimingMetadata();
        metadata.setToKeep(false);
        testToKeep(metadata, false);

        metadata = createTimingMetadata();
        metadata.setToKeep(true);
        testToKeep(metadata, true);
    }

    private TimingMetadata createTimingMetadata() {
        TimingMetadata metadata = new TimingMetadata();
        metadata.setNextCount(0L);
        metadata.setSourceCount(2L);
        metadata.setSeekCount(1L);
        metadata.setYieldCount(3L);
        metadata.setHost("localhost");
        return metadata;
    }

}
