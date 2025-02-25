package datawave.query.tables.ssdeep;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.collect.Multimap;

import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.logic.BaseQueryLogic;
import datawave.core.query.logic.QueryLogicTransformer;
import datawave.microservice.query.Query;
import datawave.query.config.SSDeepSimilarityQueryConfiguration;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.tables.ScannerFactory;
import datawave.util.ssdeep.ChunkSizeEncoding;
import datawave.util.ssdeep.IntegerEncoding;
import datawave.util.ssdeep.NGramGenerator;
import datawave.util.ssdeep.NGramTuple;
import datawave.util.ssdeep.SSDeepHash;
import datawave.webservice.query.exception.QueryException;

public class SSDeepSimilarityQueryLogic extends BaseQueryLogic<ScoredSSDeepPair> {

    private static final Logger log = Logger.getLogger(SSDeepSimilarityQueryLogic.class);

    private SSDeepSimilarityQueryConfiguration config;

    ScannerFactory scannerFactory;

    public SSDeepSimilarityQueryLogic() {
        super();
    }

    public SSDeepSimilarityQueryLogic(final SSDeepSimilarityQueryLogic ssDeepSimilarityTable) {
        super(ssDeepSimilarityTable);
        this.config = ssDeepSimilarityTable.config;
        this.scannerFactory = ssDeepSimilarityTable.scannerFactory;
    }

    @Override
    public SSDeepSimilarityQueryConfiguration getConfig() {
        if (config == null) {
            config = SSDeepSimilarityQueryConfiguration.create();
        }
        return config;
    }

    @Override
    public GenericQueryConfiguration initialize(AccumuloClient accumuloClient, Query settings, Set<Authorizations> auths) throws Exception {
        final SSDeepSimilarityQueryConfiguration config = new SSDeepSimilarityQueryConfiguration(getConfig());
        config.setState(new SSDeepSimilarityQueryState());
        config.setQuery(settings);
        config.setClient(accumuloClient);
        config.setAuthorizations(auths);
        this.scannerFactory = new ScannerFactory(config);
        setupRanges(settings, config);
        return config;
    }

    @Override
    public void setupQuery(GenericQueryConfiguration genericConfig) throws Exception {
        if (!(genericConfig instanceof SSDeepSimilarityQueryConfiguration)) {
            throw new QueryException("Did not receive a SSDeepSimilarityQueryConfiguration instance!!");
        }

        final SSDeepSimilarityQueryConfiguration config = (SSDeepSimilarityQueryConfiguration) genericConfig;

        try {
            final BatchScanner scanner = this.scannerFactory.newScanner(config.getTableName(), config.getAuthorizations(), config.getQueryThreads(),
                            config.getQuery());

            scanner.setRanges(config.getState().getRanges());

            final SSDeepParsingFunction parsingFunction = new SSDeepParsingFunction(config);
            Stream<Map.Entry<NGramTuple,SSDeepHash>> parsedStream = scanner.stream().map(parsingFunction);

            if (config.isDedupeSimilarityHashes()) {
                final SSDeepSeenFunction ssDeepDedupeFunction = new SSDeepSeenFunction();
                parsedStream = parsedStream.filter(ssDeepDedupeFunction);
            }

            if (config.getMaxHashesPerNGram() > -1) {
                final SSDeepMaxHashPerNGramFilter maxHashPerNGramLimiter = new SSDeepMaxHashPerNGramFilter(config);
                parsedStream = parsedStream.filter(maxHashPerNGramLimiter);
            }

            if (getMaxResults() > -1) {
                final AtomicLong count = new AtomicLong();
                parsedStream = parsedStream.peek(entry -> {
                    if (count.incrementAndGet() > getMaxResults()) {
                        throw new DatawaveFatalQueryException("Exceeded max work");
                    }
                });
            }

            // must be called after setRanges so that we get the query map from the config.
            final SSDeepScoringFunction scoringFunction = new SSDeepScoringFunction(config);
            Stream<ScoredSSDeepPair> scoredStream = parsedStream.flatMap(scoringFunction);

            this.iterator = scoredStream.iterator();
            this.scanner = scanner;

        } catch (TableNotFoundException e) {
            throw new RuntimeException("Table not found: " + this.getTableName(), e);
        }
    }

    /**
     * Process the query to create the ngrams for the ranges to scan in accumulo. Store these in the configs along with a map that can be used to identify which
     * SSDeepHash each query ngram originated from.
     *
     * @param settings
     *            the query we will be running.
     * @param config
     *            write ranges and query map to this object.
     */
    public void setupRanges(Query settings, SSDeepSimilarityQueryConfiguration config) {
        final String query = settings.getQuery().trim();
        Set<SSDeepHash> queries = Arrays.stream(query.split(" OR ")).map(k -> {
            final int pos = k.indexOf(":");
            return pos > 0 ? k.substring(pos + 1) : k;
        }).map(SSDeepHash::parse).collect(Collectors.toSet());

        log.info("Pre-processing " + queries.size() + " SSDeepHash queries");
        final int maxRepeatedCharacters = config.getMaxRepeatedCharacters();
        final NGramGenerator nGramEngine = new NGramGenerator(config.getNGramSize(), maxRepeatedCharacters, config.getMinHashSize());
        if (maxRepeatedCharacters > 0) {
            log.info("Normalizing SSDeepHashes to remove long runs of consecutive characters");
            queries = queries.stream().map(h -> h.normalize(maxRepeatedCharacters)).collect(Collectors.toSet());
        }

        if (config.getMaxHashes() != -1 && config.getMaxHashes() < queries.size()) {
            log.error("Query exceeds max hash limit of " + config.getMaxHashes() + " count: " + queries.size());
            throw new DatawaveFatalQueryException("Query exceeds max hash limit of " + config.getMaxHashes() + " count: " + queries.size());
        }

        final Multimap<NGramTuple,SSDeepHash> queryMap = nGramEngine.preprocessQueries(queries);
        final Set<Range> ranges = new TreeSet<>();

        final IntegerEncoding bucketEncoder = new IntegerEncoding(config.getBucketEncodingBase(), config.getBucketEncodingLength());
        final ChunkSizeEncoding chunkSizeEncoder = new ChunkSizeEncoding();

        final int indexBuckets = config.getIndexBuckets();

        if (queryMap.isEmpty()) {
            String message = "Unable to generate SSDeepHash ngrams for query: " + settings.getQuery() + ", possibly due to invalid SSDeep hash(es)?";
            log.error(message);
            throw new SSDeepRuntimeQueryException(message);
        }

        // TODO: stream?
        for (NGramTuple ct : queryMap.keys()) {
            final String sizeAndChunk = chunkSizeEncoder.encode(ct.getChunkSize()) + ct.getChunk();
            for (int i = 0; i < indexBuckets; i++) {
                final String bucketedSizeAndChunk = bucketEncoder.encode(i) + sizeAndChunk;
                ranges.add(Range.exact(new Text(bucketedSizeAndChunk)));
            }
        }

        log.info("Generated " + queryMap.size() + " SSDeepHash ngrams of size " + nGramEngine.getNgramSize() + " and " + ranges.size() + " ranges. ");
        if (log.isDebugEnabled()) {
            log.debug("Query map is: " + queryMap);
            log.debug("Ranges are: " + ranges);
        }
        config.getState().setRanges(ranges);
        config.getState().setQueryMap(queryMap);
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return new SSDeepSimilarityQueryLogic(this);
    }

    @Override
    public void close() {
        super.close();
        final ScannerFactory factory = this.scannerFactory;
        if (null == factory) {
            log.debug("ScannerFactory is null; not closing it.");
        } else {
            int nClosed = 0;
            factory.lockdown();
            for (final ScannerBase bs : factory.currentScanners()) {
                factory.close(bs);
                ++nClosed;
            }
            if (log.isDebugEnabled())
                log.debug("Cleaned up " + nClosed + " batch scanners associated with this query logic.");
        }
    }

    @Override
    public AccumuloConnectionFactory.Priority getConnectionPriority() {
        return AccumuloConnectionFactory.Priority.NORMAL;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public QueryLogicTransformer getTransformer(Query settings) {
        final SSDeepSimilarityQueryConfiguration config = getConfig();
        return new SSDeepSimilarityQueryTransformer(settings, config, this.markingFunctions, this.responseObjectFactory);
    }

    @Override
    public Set<String> getOptionalQueryParameters() {
        return Collections.emptySet();
    }

    @Override
    public Set<String> getRequiredQueryParameters() {
        return Collections.emptySet();
    }

    @Override
    public Set<String> getExampleQueries() {
        return Collections.emptySet();
    }

    public void setIndexBuckets(int indexBuckets) {
        getConfig().setIndexBuckets(indexBuckets);
    }

    public void setQueryThreads(int queryThreads) {
        getConfig().setQueryThreads(queryThreads);
    }

    public void setMaxRepeatedCharacters(int maxRepeatedCharacters) {
        getConfig().setMaxRepeatedCharacters(maxRepeatedCharacters);
    }

    public void setBucketEncodingBase(int bucketEncodingBase) {
        getConfig().setBucketEncodingBase(bucketEncodingBase);
    }

    public void setBucketEncodingLength(int bucketEncodingLength) {
        getConfig().setBucketEncodingLength(bucketEncodingLength);
    }

    public void setMaxHashes(int maxHashes) {
        getConfig().setMaxHashes(maxHashes);
    }

    public int getMaxHashes() {
        return getConfig().getMaxHashes();
    }

    public void setMaxHashesPerNGram(int maxHashesPerNGram) {
        getConfig().setMaxHashesPerNGram(maxHashesPerNGram);
    }

    public int getMaxHashesPerNGram() {
        return getConfig().getMaxHashesPerNGram();
    }

    public void setDedupeSimilarityHashes(boolean dedupeSimilarityHashes) {
        getConfig().setDedupeSimilarityHashes(dedupeSimilarityHashes);
    }

    public boolean isDedupeSimilarityHashes() {
        return getConfig().isDedupeSimilarityHashes();
    }
}
