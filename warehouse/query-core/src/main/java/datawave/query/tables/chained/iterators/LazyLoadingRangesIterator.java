package datawave.query.tables.chained.iterators;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.query.config.SSDeepSimilarityQueryConfiguration;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.tables.ScannerFactory;
import datawave.query.tables.ssdeep.SSDeepMaxHashPerNGramFilter;
import datawave.query.tables.ssdeep.SSDeepParsingFunction;
import datawave.query.tables.ssdeep.SSDeepScoringFunction;
import datawave.query.tables.ssdeep.SSDeepSeenFunction;
import datawave.query.tables.ssdeep.ScoredSSDeepPair;
import datawave.util.ssdeep.ChunkSizeEncoding;
import datawave.util.ssdeep.IntegerEncoding;
import datawave.util.ssdeep.NGramTuple;
import datawave.util.ssdeep.SSDeepHash;

/**
 * Iterator that lazily calculates ranges. The amount of ranges to be calculated at a time is configurable. This approach avoids creating all the ranges at the
 * same time and storing them all in memory for the life of the query.
 */
public class LazyLoadingRangesIterator implements Iterator<ScoredSSDeepPair> {
    private static final Logger log = LoggerFactory.getLogger(LazyLoadingRangesIterator.class);

    private final Iterator<NGramTuple> queryMapKeysIterator;
    private final ScannerFactory scannerFactory;
    private BatchScanner scanner;
    private Iterator<Map.Entry<Key,Value>> scannerIterator;
    private final int numRangesPerScanner;

    private final ChunkSizeEncoding chunkSizeEncoder = new ChunkSizeEncoding();
    private final IntegerEncoding bucketEncoder;
    private final int indexBuckets;

    private final SSDeepSimilarityQueryConfiguration config;

    private final SSDeepParsingFunction parsingFunction;
    private final SSDeepSeenFunction ssDeepDedupeFunction;
    private final SSDeepMaxHashPerNGramFilter maxHashPerNGramLimiter;
    private final AtomicLong count = new AtomicLong(0);
    private final long maxResults;
    private final SSDeepScoringFunction scoringFunction;

    private Iterator<ScoredSSDeepPair> currentScoredSSDeepPairsIterator;

    private ScoredSSDeepPair next;

    public LazyLoadingRangesIterator(SSDeepSimilarityQueryConfiguration ssDeepSimilarityQueryConfiguration, ScannerFactory scannerFactory, long maxResults)
                    throws TableNotFoundException {
        config = ssDeepSimilarityQueryConfiguration;
        this.maxResults = maxResults;
        queryMapKeysIterator = config.getState().getQueryMap().keys().iterator();
        bucketEncoder = new IntegerEncoding(config.getBucketEncodingBase(), config.getBucketEncodingLength());
        indexBuckets = config.getIndexBuckets();
        numRangesPerScanner = config.getNumRangesPerScanner();

        this.scannerFactory = scannerFactory;

        parsingFunction = new SSDeepParsingFunction(config);
        ssDeepDedupeFunction = new SSDeepSeenFunction();
        maxHashPerNGramLimiter = new SSDeepMaxHashPerNGramFilter(config);
        scoringFunction = new SSDeepScoringFunction(config);
    }

    /**
     * Continue creating new iterators using a new calculated set of ranges until a next result can be found
     *
     * @return true if a result has been found, false if not
     */
    @Override
    public boolean hasNext() {
        if (next != null) {
            return true;
        }

        boolean foundNext = false;
        while (!foundNext) {
            if (currentScoredSSDeepPairsIterator == null || !currentScoredSSDeepPairsIterator.hasNext()) {
                while (scannerIterator == null || !scannerIterator.hasNext()) {
                    if (queryMapKeysIterator.hasNext()) {
                        try {
                            scannerIterator = createIteratorWithNewRanges();
                        } catch (TableNotFoundException e) {
                            throw new RuntimeException(e);
                        }
                    } else {
                        return false;
                    }
                }

                Map.Entry<Key,Value> entry = scannerIterator.next();

                Map.Entry<NGramTuple,SSDeepHash> simplifiedEntry = parsingFunction.apply(entry);

                if (config.isDedupeSimilarityHashes() && !ssDeepDedupeFunction.test(simplifiedEntry)) {
                    continue;
                }

                if (config.getMaxHashesPerNGram() > -1 && !maxHashPerNGramLimiter.test(simplifiedEntry)) {
                    continue;
                }

                if (maxResults > -1 && count.incrementAndGet() > maxResults) {
                    throw new DatawaveFatalQueryException("Exceeded max work");
                }

                if (currentScoredSSDeepPairsIterator == null || !currentScoredSSDeepPairsIterator.hasNext()) {
                    currentScoredSSDeepPairsIterator = scoringFunction.apply(simplifiedEntry).iterator();
                }

                if (currentScoredSSDeepPairsIterator.hasNext()) {
                    foundNext = true;
                    next = currentScoredSSDeepPairsIterator.next();
                }
            } else {
                foundNext = true;
                next = currentScoredSSDeepPairsIterator.next();
            }
        }

        return true;
    }

    /**
     * Return next and reset
     *
     * @return the next item
     */
    @Override
    public ScoredSSDeepPair next() {
        ScoredSSDeepPair scoredPair = next;
        next = null;
        return scoredPair;
    }

    /**
     * Process the query map keys to create ranges to scan in Accumulo. The number of ranges generated at a time is determined by {@code numRangesPerScanner}.
     * The number of ranges may go over numRangesPerScanner slightly due to processing all index buckets per NGramTuple
     *
     * @return an iterator based on the next set of calculated ranges
     */
    private Iterator<Map.Entry<Key,Value>> createIteratorWithNewRanges() throws TableNotFoundException {
        final Collection<Range> ranges = new HashSet<>();

        while (queryMapKeysIterator.hasNext() && ranges.size() <= numRangesPerScanner) {
            NGramTuple ct = queryMapKeysIterator.next();
            final String sizeAndChunk = chunkSizeEncoder.encode(ct.getChunkSize()) + ct.getChunk();
            for (int i = 0; i < indexBuckets; i++) {
                final String bucketedSizeAndChunk = bucketEncoder.encode(i) + sizeAndChunk;
                ranges.add(Range.exact(new Text(bucketedSizeAndChunk)));
            }
        }

        if (scanner != null) {
            scanner.close();
        }
        scanner = scannerFactory.newScanner(config.getTableName(), config.getAuthorizations(), config.getQueryThreads(), config.getQuery());

        scanner.setRanges(ranges);

        log.debug("Lazy loaded {} ranges.", ranges.size());
        log.trace("Ranges are: {}", ranges);

        return scanner.stream().iterator();
    }
}
