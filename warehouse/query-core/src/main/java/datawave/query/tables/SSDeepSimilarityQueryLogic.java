package datawave.query.tables;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;

import datawave.query.config.SSDeepSimilarityQueryConfiguration;
import datawave.query.transformer.SSDeepSimilarityQueryTransformer;
import datawave.query.util.ssdeep.ChunkSizeEncoding;
import datawave.query.util.ssdeep.IntegerEncoding;
import datawave.query.util.ssdeep.NGramGenerator;
import datawave.query.util.ssdeep.NGramTuple;
import datawave.query.util.ssdeep.SSDeepHash;
import datawave.webservice.common.connection.AccumuloConnectionFactory;
import datawave.webservice.query.Query;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.logic.BaseQueryLogic;
import datawave.webservice.query.logic.QueryLogicTransformer;

public class SSDeepSimilarityQueryLogic extends BaseQueryLogic<Map.Entry<Key,Value>> {

    private static final Logger log = Logger.getLogger(SSDeepSimilarityQueryLogic.class);

    /** Used to store the map of ssdeep chnuks mapped to the original ssdeep hash in the Query's optional parameters map */
    public static final String SSDEEP_QUERY_MAP_PARAM = "ssdeepQueryMap";

    int indexBuckets = 32;

    int queryThreads = 100;

    int maxRepeatedCharacters = 3;

    int bucketEncodingBase = 32;

    int bucketEncodingLength = 2;

    /** Used to encode buckets as characters which are prepended to the ranges used to retireve ngram tuples */
    private IntegerEncoding bucketEncoder;
    /** Used to encode the chunk size as a character which is included in the ranges used to retrieve ngram tuples */
    private ChunkSizeEncoding chunkSizeEncoding;

    ScannerFactory scannerFactory;

    public SSDeepSimilarityQueryLogic() {
        super();
    }

    public SSDeepSimilarityQueryLogic(final SSDeepSimilarityQueryLogic ssDeepSimilarityTable) {
        super(ssDeepSimilarityTable);
        this.scannerFactory = ssDeepSimilarityTable.scannerFactory;
        this.bucketEncoder = ssDeepSimilarityTable.bucketEncoder;
        this.chunkSizeEncoding = ssDeepSimilarityTable.chunkSizeEncoding;
        this.bucketEncodingLength = ssDeepSimilarityTable.bucketEncodingLength;
        this.bucketEncodingBase = ssDeepSimilarityTable.bucketEncodingBase;
        this.maxRepeatedCharacters = ssDeepSimilarityTable.maxRepeatedCharacters;
        this.queryThreads = ssDeepSimilarityTable.queryThreads;
        this.indexBuckets = ssDeepSimilarityTable.indexBuckets;
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
    public GenericQueryConfiguration initialize(AccumuloClient accumuloClient, Query settings, Set<Authorizations> auths) throws Exception {
        final SSDeepSimilarityQueryConfiguration config = new SSDeepSimilarityQueryConfiguration(this, settings);
        this.scannerFactory = new ScannerFactory(accumuloClient);
        this.bucketEncoder = new IntegerEncoding(bucketEncodingBase, bucketEncodingLength);
        this.chunkSizeEncoding = new ChunkSizeEncoding();

        config.setClient(accumuloClient);
        config.setAuthorizations(auths);
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
            final BatchScanner scanner = this.scannerFactory.newScanner(config.getTableName(), config.getAuthorizations(), this.queryThreads,
                            config.getQuery());
            scanner.setRanges(config.getRanges());
            this.iterator = scanner.iterator();
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

        final NGramGenerator nGramEngine = new NGramGenerator(7);
        log.info("Pre-processing " + queries.size() + " SSDeepHash queries");
        if (this.maxRepeatedCharacters > 0) {
            log.info("Normalizing SSDeepHashes to remove long runs of consecutive characters");
            queries = queries.stream().map(h -> h.normalize(maxRepeatedCharacters)).collect(Collectors.toSet());
        }
        final Multimap<NGramTuple,SSDeepHash> queryMap = nGramEngine.preprocessQueries(queries);
        final Set<Range> ranges = new TreeSet<>();
        for (NGramTuple ct : queryMap.keys()) {
            final String sizeAndChunk = chunkSizeEncoding.encode(ct.getChunkSize()) + ct.getChunk();
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
        config.setRanges(ranges);
        config.setQueryMap(queryMap);

        Map<String,List<String>> optionalQueryParameters = settings.getOptionalQueryParameters();
        if (optionalQueryParameters == null) {
            optionalQueryParameters = new HashMap<>();
            settings.setOptionalQueryParameters(optionalQueryParameters);
        }

        // TODO: update optionalQueryParameters map with contents of queryMap;
        List<String> queryMapList = encodeQueryMap(queryMap);
        optionalQueryParameters.put(SSDEEP_QUERY_MAP_PARAM, queryMapList);
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return new SSDeepSimilarityQueryLogic(this);
    }

    @Override
    public AccumuloConnectionFactory.Priority getConnectionPriority() {
        return AccumuloConnectionFactory.Priority.NORMAL;
    }

    @Override
    public QueryLogicTransformer getTransformer(Query settings) {
        SSDeepSimilarityQueryTransformer transformer = new SSDeepSimilarityQueryTransformer(settings, this.markingFunctions, this.responseObjectFactory);
        // transformer.setQueryMap(settings.getQueryMap());
        return transformer;
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

    public int getIndexBuckets() {
        return indexBuckets;
    }

    public void setIndexBuckets(int indexBuckets) {
        this.indexBuckets = indexBuckets;
    }

    public int getQueryThreads() {
        return queryThreads;
    }

    public void setQueryThreads(int queryThreads) {
        this.queryThreads = queryThreads;
    }

    public int getMaxRepeatedCharacters() {
        return maxRepeatedCharacters;
    }

    public void setMaxRepeatedCharacters(int maxRepeatedCharacters) {
        this.maxRepeatedCharacters = maxRepeatedCharacters;
    }

    public int getBucketEncodingBase() {
        return bucketEncodingBase;
    }

    public void setBucketEncodingBase(int bucketEncodingBase) {
        this.bucketEncodingBase = bucketEncodingBase;
    }

    public int getBucketEncodingLength() {
        return bucketEncodingLength;
    }

    public void setBucketEncodingLength(int bucketEncodingLength) {
        this.bucketEncodingLength = bucketEncodingLength;
    }

    /**
     * Parse a List of encoded query map entries back into the original query map
     *
     * @param queryMapList
     *            the encoded query map in list form
     * @return the decoded query map.
     */
    public static Multimap<NGramTuple,SSDeepHash> decodeQueryMapList(List<String> queryMapList) {
        Multimap<NGramTuple,SSDeepHash> queryMap = TreeMultimap.create();

        for (String e : queryMapList) {
            int pos = e.indexOf("##");
            if (pos > 0) {
                String tupleString = e.substring(0, pos);
                // TODO catch parse failure
                NGramTuple tuple = NGramTuple.parse(tupleString);

                int currentPos = pos + 2;
                while (currentPos > pos && currentPos < e.length()) {
                    int nextPos = e.indexOf("%%", currentPos);
                    if (nextPos > currentPos) {
                        String ssdeepString = e.substring(currentPos, nextPos);
                        // TODO catch parse failure
                        SSDeepHash hash = SSDeepHash.parse(ssdeepString);
                        queryMap.put(tuple, hash);
                        currentPos = nextPos + 2;
                    } else {
                        break;
                    }
                }
            } else {
                throw new IllegalStateException("Could not parse ssdeep queryMap from parameter list entry: " + e);
            }
        }

        log.info("Decoded ssdeep query map of size " + queryMap.size() + " from parameter list of length " + queryMapList.size());
        return queryMap;
    }

    /**
     * Encode the query map into a list of strings, each item in the list corresponds to an entry in the query map
     *
     * @param queryMap
     *            the query map to encode
     * @return the encoded query map represented as a list of strings
     */
    public static List<String> encodeQueryMap(Multimap<NGramTuple,SSDeepHash> queryMap) {
        final List<String> queryMapList = new ArrayList<>();
        for (Map.Entry<NGramTuple,Collection<SSDeepHash>> e : queryMap.asMap().entrySet()) {
            final StringBuilder b = new StringBuilder();
            final NGramTuple t = e.getKey();
            b.append(t.toString()).append("##");

            final Collection<SSDeepHash> ss = e.getValue();
            for (SSDeepHash s : ss) {
                b.append(s.toString()).append("%%");
            }
            queryMapList.add(b.toString());
        }
        log.info("Encoded ssdeep query map of size " + queryMap.size() + " into parameter list of length " + queryMapList.size());
        return queryMapList;
    }
}
