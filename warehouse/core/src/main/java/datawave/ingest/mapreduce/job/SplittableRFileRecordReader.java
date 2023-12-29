package datawave.ingest.mapreduce.job;

import static org.apache.accumulo.core.conf.Property.TABLE_CRYPTO_PREFIX;

import java.io.EOFException;
import java.io.IOException;
import java.util.Collections;

import org.apache.accumulo.core.crypto.CryptoFactoryLoader;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.spi.crypto.CryptoEnvironment;
import org.apache.accumulo.core.spi.crypto.CryptoService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class SplittableRFileRecordReader extends RFileRecordReader {
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        if (split instanceof RFileSplit) {
            fileIterator = getIterator(context.getConfiguration(), (RFileSplit) split);
        } else {
            super.initialize(split, context);
        }
    }

    public static RFile.Reader getRFileReader(Configuration config, Path rfile) throws IOException {
        FileSystem fs = rfile.getFileSystem(config);

        if (!fs.exists(rfile)) {
            throw new IllegalArgumentException(rfile + " does not exist");
        }

        CryptoService cs = CryptoFactoryLoader.getServiceForClient(CryptoEnvironment.Scope.TABLE, config.getPropsWithPrefix(TABLE_CRYPTO_PREFIX.name()));
        CachableBlockFile.CachableBuilder cb = new CachableBlockFile.CachableBuilder().fsPath(fs, rfile).conf(config).cryptoService(cs);
        return new RFile.Reader(cb);
    }

    /**
     * Open an rfile specified by the split, create an iterator to read the region of the rfile configured in the split by reading the rfile index blocks.
     * Delegate to the split to get the seek range.
     *
     * @param config
     * @param split
     * @return
     * @throws IOException
     */
    public static FileSKVIterator getIterator(Configuration config, RFileSplit split) throws IOException {
        Path rfile = split.getPath();
        long startIndexBlock = split.getStartBlock();
        long numIndexBlocks = split.getNumBlocks();

        RFile.Reader rfileReader = getRFileReader(config, rfile);
        FileSKVIterator iter = rfileReader.getIndex();

        Key start = null;
        int blkCount = 1;
        if (startIndexBlock > 0) {
            while (iter.hasTop() && blkCount < startIndexBlock) {
                iter.next();
                blkCount++;
            }

            if (blkCount < startIndexBlock) {
                throw new EOFException("start index " + startIndexBlock + " beyond end of file");
            }

            start = iter.getTopKey();
            blkCount = 0;
        }

        // find the end key
        while (iter.hasTop() && blkCount < numIndexBlocks) {
            iter.next();
            blkCount++;
        }

        if (blkCount < numIndexBlocks) {
            throw new EOFException("end index " + startIndexBlock + numIndexBlocks + " beyond end of file");
        }

        Key end = null;
        if (iter.hasTop()) {
            end = iter.getTopKey();
        }

        Range seekRange = split.getSeekRange(start, end);

        rfileReader.seek(seekRange, Collections.EMPTY_SET, false);

        return rfileReader;
    }
}
