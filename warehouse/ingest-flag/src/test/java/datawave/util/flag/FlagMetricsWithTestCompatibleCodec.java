package datawave.util.flag;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.DefaultCodec;

public class FlagMetricsWithTestCompatibleCodec extends FlagMetrics {
    public FlagMetricsWithTestCompatibleCodec(FileSystem fs, boolean isEnabled) {
        super(fs, isEnabled);
    }

    @Override
    SequenceFile.Writer.Option getCompressionOption() {
        return SequenceFile.Writer.compression(SequenceFile.CompressionType.NONE, new DefaultCodec());
    }
}
