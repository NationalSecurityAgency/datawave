package datawave.ingest.mapreduce.job.util;

import static org.apache.accumulo.core.conf.Property.TABLE_CRYPTO_PREFIX;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.rfile.RFileSource;
import org.apache.accumulo.core.crypto.CryptoFactoryLoader;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.system.MultiIterator;
import org.apache.accumulo.core.spi.crypto.CryptoEnvironment;
import org.apache.accumulo.core.spi.crypto.CryptoService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Collection of utilities to locate, read, and split rfiles
 */
public class RFileUtil {
    private Configuration config;

    public RFileUtil(Configuration config) {
        this.config = config;
    }

    public Scanner getRFileScanner(String... paths) {
        return getRFileScanner(config, paths);
    }

    public RFile.Reader getRFileReader(Path rfile) throws IOException {
        return getRFileReader(config, rfile);
    }

    public List<Range> getRangeSplits(String paths, Key start, Key end, int indexBlocksPerSplit) throws IOException {
        return getRangeSplits(config, paths, start, end, indexBlocksPerSplit);
    }

    public List<Range> getRangeSplits(String paths, Key start, Key end, int indexBlocksPerSplit, Function<Key,Key> rangeAdjuster) throws IOException {
        return getRangeSplits(config, paths, start, end, indexBlocksPerSplit, rangeAdjuster);
    }

    public List<Range> getRangeSplits(String[] paths, Key start, Key end, int indexBlocksPerSplit, Function<Key,Key> rangeAdjuster) throws IOException {
        return getRangeSplits(config, paths, start, end, indexBlocksPerSplit, rangeAdjuster);
    }

    public List<Range> getRangeSplits(Collection<String> paths, Key start, Key end, int indexBlocksPerSplit, Function<Key,Key> rangeAdjuster)
                    throws IOException {
        return getRangeSplits(config, paths, start, end, indexBlocksPerSplit, rangeAdjuster);
    }

    public static Scanner getRFileScanner(Configuration config, String... paths) {
        RFileSource[] sources = new RFileSource[paths.length];
        try {
            for (int i = 0; i < paths.length; i++) {
                Path p = new Path(paths[i]);
                FileSystem fs = FileSystem.get(p.toUri(), config);
                if (!fs.exists(p)) {
                    throw new IOException("file not found " + p);
                }
                sources[i] = new RFileSource(fs.open(p), fs.getFileStatus(p).getLen());
            }
        } catch (IOException e) {
            throw new RuntimeException("could not open source rfiles: " + paths, e);
        }

        return org.apache.accumulo.core.client.rfile.RFile.newScanner().from(sources).withoutSystemIterators().build();
    }

    public static RFile.Reader getRFileReader(Configuration config, Path rfile) throws IOException {
        FileSystem fs = rfile.getFileSystem(config);
        if (!fs.exists(rfile)) {
            throw new FileNotFoundException(rfile + " does not exist");
        }

        CryptoService cs = CryptoFactoryLoader.getServiceForClient(CryptoEnvironment.Scope.TABLE, config.getPropsWithPrefix(TABLE_CRYPTO_PREFIX.name()));
        CachableBlockFile.CachableBuilder cb = new CachableBlockFile.CachableBuilder().fsPath(fs, rfile).conf(config).cryptoService(cs);

        return new RFile.Reader(cb);
    }

    public static List<Range> getRangeSplits(Configuration config, String paths, Key start, Key end, int indexBlocksPerSplit) throws IOException {
        return getRangeSplits(config, paths, start, end, indexBlocksPerSplit, Function.identity());
    }

    public static List<Range> getRangeSplits(Configuration config, String paths, Key start, Key end, int indexBlocksPerSplit, Function<Key,Key> rangeAdjuster)
                    throws IOException {
        return getRangeSplits(config, paths.split(","), start, end, indexBlocksPerSplit, rangeAdjuster);
    }

    public static List<Range> getRangeSplits(Configuration config, String[] paths, Key start, Key end, int indexBlocksPerSplit, Function<Key,Key> rangeAdjuster)
                    throws IOException {
        return getRangeSplits(config, Arrays.asList(paths), start, end, indexBlocksPerSplit, rangeAdjuster);
    }

    public static List<Range> getRangeSplits(Configuration config, Collection<String> paths, Key start, Key end, int indexBlocksPerSplit,
                    Function<Key,Key> rangeAdjuster) throws IOException {
        List<Range> splits = new ArrayList<>();
        List<RFile.Reader> readers = new ArrayList<>();
        try {
            // track the readers so they can be closed after use
            List<SortedKeyValueIterator<Key,Value>> indexIterators = new ArrayList<>();
            Key lastKey = null;
            Key startKey = null;
            for (String rfile : paths) {
                RFile.Reader reader = getRFileReader(config, new Path(rfile));
                if (lastKey == null || reader.getLastKey().compareTo(lastKey) > 0) {
                    lastKey = reader.getLastKey();
                }
                if (startKey == null || reader.getFirstKey().compareTo(startKey) < 0) {
                    startKey = reader.getFirstKey();
                }
                readers.add(reader);
                // get the index iterator from the reader
                SortedKeyValueIterator indexIterator = reader.getIndex();
                indexIterators.add(indexIterator);
            }

            // if none of the rfiles have any keys return an empty set
            if (lastKey == null) {
                return Collections.emptyList();
            }

            Key fenceStart = start;
            Key fenceEnd = end;

            Range fence = new Range(fenceStart, true, fenceEnd, true);

            // move the fence in if the start key and end key are not at the fence
            if (fence.contains(startKey)) {
                fenceStart = startKey;
                fence = new Range(fenceStart, true, fenceEnd, true);
            }

            if (fence.contains(lastKey)) {
                fenceEnd = lastKey;
            }

            fence = new Range(fenceStart, true, fenceEnd, true);

            MultiIterator wrappedIterator = new MultiIterator(indexIterators, true);

            Key splitStart = fenceStart;

            int blockCount = 0;
            while (wrappedIterator.hasTop()) {
                Key top = wrappedIterator.getTopKey();
                if (fence.beforeStartKey(top)) {
                    wrappedIterator.next();
                    continue;
                } else if (fence.afterEndKey(top)) {
                    break;
                }

                blockCount++;
                if (blockCount > indexBlocksPerSplit) {
                    Key splitEnd = rangeAdjuster.apply(top);

                    if (!splitEnd.equals(splitStart)) {
                        splits.add(new Range(splitStart, true, splitEnd, false));
                        splitStart = splitEnd;
                        blockCount = 0;
                    }
                }

                wrappedIterator.next();
            }

            if (blockCount > 0) {
                splits.add(new Range(splitStart, true, fence.getEndKey(), fence.isEndKeyInclusive()));
            }
        } finally {
            // close all readers
            for (RFile.Reader reader : readers) {
                reader.close();
            }
        }

        return splits;
    }
}
