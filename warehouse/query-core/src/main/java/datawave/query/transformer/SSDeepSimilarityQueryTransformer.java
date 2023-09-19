package datawave.query.transformer;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;

import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;

import datawave.marking.MarkingFunctions;
import datawave.query.config.SSDeepSimilarityQueryConfiguration;
import datawave.query.util.ssdeep.ChunkSizeEncoding;
import datawave.query.util.ssdeep.IntegerEncoding;
import datawave.query.util.ssdeep.NGramScoreTuple;
import datawave.query.util.ssdeep.NGramTuple;
import datawave.query.util.ssdeep.SSDeepHash;
import datawave.webservice.query.Query;
import datawave.webservice.query.exception.EmptyObjectException;
import datawave.webservice.query.logic.BaseQueryLogicTransformer;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.FieldBase;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.EventQueryResponseBase;

public class SSDeepSimilarityQueryTransformer extends BaseQueryLogicTransformer<Map.Entry<Key,Value>,Map.Entry<SSDeepHash,NGramTuple>> {

    private static final Logger log = Logger.getLogger(SSDeepSimilarityQueryTransformer.class);

    /** The number of characters in the bucket encoding alphabet */
    public static final int BUCKET_ENCODING_BASE = 32;
    /** The length of the bucket encoding we will perform */
    public static final int BUCKET_ENCODING_LENGTH = 2;

    protected final Authorizations auths;

    protected final ResponseObjectFactory responseObjectFactory;

    /** Used to encode the chunk size as a character which is included in the ranges used to retrieve ngram tuples */
    final ChunkSizeEncoding chunkSizeEncoding;

    /** Used to encode buckets as characters which are prepended to the ranges used to retrieve ngram tuples */
    final IntegerEncoding bucketEncoder;

    /**
     * the position where the ngram will start in the generated ranges, determined at construction time based on the bucketEncoder parameters
     */
    final int chunkStart;
    /**
     * the position where the query ngram will end in the generate ranges, determined based on the bucketEncoder and chunkSizeEncoding parameters
     */

    final int chunkEnd;

    /** Tracks which ssdeep hashes each of the ngrams originated from */
    final Multimap<NGramTuple,SSDeepHash> queryMap;

    public SSDeepSimilarityQueryTransformer(Query query, SSDeepSimilarityQueryConfiguration config, MarkingFunctions markingFunctions,
                    ResponseObjectFactory responseObjectFactory) {
        super(markingFunctions);
        this.auths = new Authorizations(query.getQueryAuthorizations().split(","));
        this.queryMap = config.getQueryMap();
        this.responseObjectFactory = responseObjectFactory;

        this.bucketEncoder = new IntegerEncoding(config.getBucketEncodingBase(), config.getBucketEncodingLength());
        this.chunkSizeEncoding = new ChunkSizeEncoding();

        this.chunkStart = bucketEncoder.getLength();
        this.chunkEnd = chunkStart + chunkSizeEncoding.getLength();
    }

    @Override
    public BaseQueryResponse createResponse(List<Object> resultList) {
        Multimap<SSDeepHash,NGramTuple> mm = TreeMultimap.create();
        for (Object o : resultList) {
            Map.Entry<SSDeepHash,NGramTuple> e = (Map.Entry<SSDeepHash,NGramTuple>) o;
            mm.put(e.getKey(), e.getValue());
        }
        Multimap<SSDeepHash,NGramScoreTuple> scoreTuples = scoreQuery(queryMap, mm);
        return generateResponseFromScores(scoreTuples);
    }

    public BaseQueryResponse generateResponseFromScores(Multimap<SSDeepHash,NGramScoreTuple> scoreTuples) {
        // package the scoredTuples into an event query response
        final EventQueryResponseBase eventResponse = responseObjectFactory.getEventQueryResponse();
        final List<EventBase> events = new ArrayList<>();

        SSDeepHash lastHash = null;
        int rank = 1;
        for (Map.Entry<SSDeepHash,NGramScoreTuple> e : scoreTuples.entries()) {
            if (!e.getKey().equals(lastHash)) {
                log.info("New query hash: " + e.getKey());
                rank = 1;
                lastHash = e.getKey();
            }

            final EventBase event = responseObjectFactory.getEvent();
            final List<FieldBase> fields = new ArrayList<>();

            FieldBase f = responseObjectFactory.getField();
            f.setName("MATCHING_SSDEEP");
            f.setValue(e.getValue().getSsDeepHash().toString());
            fields.add(f);

            f = responseObjectFactory.getField();
            f.setName("QUERY_SSDEEP");
            f.setValue(lastHash.toString());
            fields.add(f);

            f = responseObjectFactory.getField();
            f.setName("MATCH_SCORE");
            f.setValue(String.valueOf(e.getValue().getScore()));
            fields.add(f);

            f = responseObjectFactory.getField();
            f.setName("MATCH_RANK");
            f.setValue(String.valueOf(rank));
            fields.add(f);

            event.setFields(fields);
            events.add(event);

            log.info("    " + rank + ". " + e.getValue());
            rank++;
        }

        eventResponse.setEvents(events);

        return eventResponse;
    }

    @Override
    public Map.Entry<SSDeepHash,NGramTuple> transform(Map.Entry<Key,Value> input) throws EmptyObjectException {
        // We will receive entries like:
        // +++//thPkK 3:3:yionv//thPkKlDtn/rXScG2/uDlhl2UE9FQEul/lldDpZflsup:6v/lhPkKlDtt/6TIPFQEqRDpZ+up []

        final Key k = input.getKey();
        final String row = k.getRow().toString();

        // extract the matching ngram and chunk size from the rowId.
        int chunkSize = chunkSizeEncoding.decode(row.substring(chunkStart, chunkEnd));
        String ngram = row.substring(chunkEnd);

        final NGramTuple c = new NGramTuple(chunkSize, ngram);

        // extract the matching ssdeep hash from the column qualifier
        final String s = k.getColumnQualifier().toString();
        try {
            final SSDeepHash h = SSDeepHash.parse(s);
            return new AbstractMap.SimpleImmutableEntry<>(h, c);
        } catch (Exception ioe) {
            log.warn(ioe.getMessage() + " when parsing: " + s);
        }

        return null;
    }

    /**
     * Given query ngrams and matching ssdeep hashes, return a scored set of ssdeep hashes that match the query and their accompanying scores.
     *
     * @param queryMap
     *            a map of ngrams to the query ssdeep hashes from which they originate.
     * @param chunkPostings
     *            a map of matching ssdeep hashes liked to the ngram tuple that was matched.
     * @return a map of ssdeep hashes to score tuples.
     */
    protected Multimap<SSDeepHash,NGramScoreTuple> scoreQuery(Multimap<NGramTuple,SSDeepHash> queryMap, Multimap<SSDeepHash,NGramTuple> chunkPostings) {
        // score based on chunk match count
        final Map<SSDeepHash,Map<SSDeepHash,Integer>> scoredHashMatches = new TreeMap<>();

        // align the chunk postings to their original query ssdeep hash and count the number of matches
        // for each chunk that corresponds to that original ssdeep hash
        chunkPostings.asMap().forEach((hash, cpc) -> {
            log.trace("Posting " + hash + " had " + cpc.size() + "chunk tuples");
            cpc.forEach(ct -> {
                Collection<SSDeepHash> ssdhc = queryMap.get(ct);
                log.trace("Chunk tuple " + ct + " had " + ssdhc.size() + "related query hashes");
                ssdhc.forEach(ssdh -> {
                    final Map<SSDeepHash,Integer> chunkMatchCount = scoredHashMatches.computeIfAbsent(ssdh, s -> new TreeMap<>());
                    final Integer score = chunkMatchCount.computeIfAbsent(hash, m -> 0);
                    log.trace("Incrementing score for " + ssdh + "," + hash + " by " + cpc.size());
                    chunkMatchCount.put(hash, score + 1);
                });
            });
        });

        // convert the counted chunks into tuples.
        final Multimap<SSDeepHash,NGramScoreTuple> scoreTuples = TreeMultimap.create();
        scoredHashMatches.forEach((sdh, cmc) -> cmc.forEach((k, v) -> scoreTuples.put(sdh, new NGramScoreTuple(k, v))));
        return scoreTuples;
    }

    public Multimap<NGramTuple,SSDeepHash> getQueryMap() {
        return queryMap;
    }
}
