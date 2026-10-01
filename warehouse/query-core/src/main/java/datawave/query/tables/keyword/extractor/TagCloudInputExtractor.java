package datawave.query.tables.keyword.extractor;

import java.util.Map;

import org.apache.accumulo.core.data.Key;

import com.google.common.base.Preconditions;

import datawave.microservice.query.Query;
import datawave.query.attributes.Attribute;
import datawave.query.tables.keyword.transform.TagCloudInputTransformer;
import datawave.util.keyword.TagCloudPartition;

/**
 * Interface for extracting tag cloud input data from Key/Value pairs during keyword query processing.
 * <p>
 * Tag cloud extractors are responsible for transforming document field data into {@link TagCloudPartition} objects. Implementations extract relevant fields
 * from document data, apply scoring logic, and accumulate the results into partitions for tag clouds.
 * <p>
 * The extraction lifecycle follows this pattern:
 * <ol>
 * <li>Optional initialization via {@link #initialize(Query)} with query settings</li>
 * <li>Repeated calls to {@link #extract(Key, Map)} for each document</li>
 * <li>Retrieval of accumulated results via {@link #get()}</li>
 * <li>Reset state via {@link #clear()} when moving to the next partition</li>
 * </ol>
 *
 * @see TagCloudPartition
 * @see TagCloudInputTransformer
 * @see FieldedTagCloudInputExtractor
 * @see ParameterFieldedTagCloudInputExtractor
 */
public interface TagCloudInputExtractor {
    /**
     * Extracts a document identifier from an event Key in the shard table
     * <p>
     * The document ID is constructed from the shard row key and column family, which follows the format:
     * <ul>
     * <li>row: yyyymmDD_X (e.g. 20260303_0)</li>
     * <li>cf: dataType\0uid</li>
     * </ul>
     * The resulting document id format is: {@code shardId/dataType/uid}
     *
     * @param source
     *            the shard event Key
     * @return document identifier in the format "shardId/dataType/uid"
     * @throws IllegalArgumentException
     *             if the cf does not contain the null byte separator
     */
    default String getDocId(Key source) {
        String row = source.getRow().toString();
        String cf = source.getColumnFamily().toString();
        int index = cf.indexOf("\0");
        Preconditions.checkArgument(-1 != index);

        String dataType = cf.substring(0, index);
        String uid = cf.substring(index + 1);

        return row + "/" + dataType + "/" + uid;
    }

    /**
     * Initialize the extractor with query-specific settings
     * <p>
     * This optional hook allows implementations to configure themselves based on the query parameters before extraction begins. By default, does nothing
     *
     * @param settings
     */
    default void initialize(Query settings) {}

    /**
     * Returns the name or category identifier of this extractor
     * <p>
     * The name is used to identify the partition category and group related tag cloud results
     *
     * @return the extractor name or category identifier
     */
    String getName();

    /**
     * Returns the name or category subtype identifier
     * <p>
     * This name is used to create subgroups of data within a given name or category when creating tag cloud results
     * <p>
     * This is an optional parameter
     *
     * @return the optional subType name or category if set, otherwise null
     */
    String getSubType();

    /**
     * Extracts tag cloud input data from a single document's fielded data
     * <p>
     * This method processes the document's fields, applies scoring logic, and accumulates the results into an internal partition. The accumulated data can be
     * retrieved via {@link #get()}
     *
     * @param source
     *            the Key for the document
     * @param documentData
     *            the document's field data
     * @throws TagCloudInputExtractorException
     *             if extraction fails due to malformed data or configuration issues
     * @see #get()
     * @see #clear()
     */
    void extract(Key source, Map<String,Attribute<? extends Comparable<?>>> documentData) throws TagCloudInputExtractorException;

    /**
     * Retrieves the accumulated tag cloud partition containing all extracted data
     * <p>
     * This method returns the partition built up through repeated calls to {@link #extract(Key, Map)}. The partition contains aggregated term frequencies and
     * scores across all processed documents
     *
     * @return the accumulated tag cloud partition, or null if no data has been extracted
     * @see #extract(Key, Map)
     * @see #clear()
     */
    TagCloudPartition get();

    /**
     * Clears the internal state and resets the accumulated partition
     * <p>
     * This method should be called after retrieving results via {@link #get()} to prepare the extractor for processing the next partition
     *
     * @see #get()
     */
    void clear();

    /**
     * Returns the transformer used to convert the partition into the final output format
     * <p>
     * The transformer is responsible for converting the accumulated {@link TagCloudPartition} into the appropriate response format
     *
     * @return the input transformer for this extractor's partition type
     * @see TagCloudInputTransformer
     * @see TagCloudPartition
     */
    TagCloudInputTransformer<TagCloudPartition> getInputTransformer();
}
