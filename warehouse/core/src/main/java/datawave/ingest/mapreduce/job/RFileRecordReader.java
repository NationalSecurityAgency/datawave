package datawave.ingest.mapreduce.job;

import static org.apache.accumulo.core.conf.Property.TABLE_CRYPTO_PREFIX;

import java.io.IOException;

import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.crypto.CryptoFactoryLoader;
import org.apache.accumulo.core.spi.crypto.CryptoEnvironment;
import org.apache.accumulo.core.spi.crypto.CryptoService;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.rfile.RFileOperations;

public class RFileRecordReader extends RecordReader<Key,Value> {
    private FileSKVIterator fileIterator;
    private boolean readFirstKeyValue = false;
    private long start, end, pos;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) split;

        // Note that the RFileInputFormat returns false for "isSplittable", so this should ALWAYS be 0
        start = fileSplit.getStart();
        if (start != 0) {
            throw new IOException("Cannot start reading an RFile in the middle: start=" + start);
        }
        end = fileSplit.getLength() - start;
        pos = start;

        FileOperations ops = RFileOperations.getInstance();
        String file = fileSplit.getPath().toString();
        FileSystem fs = fileSplit.getPath().getFileSystem(context.getConfiguration());
        CryptoService cs = CryptoFactoryLoader.getServiceForClient(CryptoEnvironment.Scope.TABLE,
                        context.getConfiguration().getPropsWithPrefix(TABLE_CRYPTO_PREFIX.name()));
        fileIterator = ops.newReaderBuilder().forFile(file, fs, context.getConfiguration(), cs).withTableConfiguration(DefaultConfiguration.getInstance())
                        .seekToBeginning().build();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        // Iterators start out on the first key, whereas record readers are
        // assumed to start on nothing and move to the first key, so we don't
        // want to advance the iterator the first time through.
        if (readFirstKeyValue) {
            fileIterator.next();
        }
        pos++;
        readFirstKeyValue = true;
        return fileIterator.hasTop();
    }

    @Override
    public Key getCurrentKey() throws IOException, InterruptedException {
        return fileIterator.getTopKey();
    }

    @Override
    public Value getCurrentValue() throws IOException, InterruptedException {
        return fileIterator.getTopValue();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - start) / (float) (end - start));
        }
    }

    @Override
    public void close() throws IOException {
        fileIterator.close();
    }

}
