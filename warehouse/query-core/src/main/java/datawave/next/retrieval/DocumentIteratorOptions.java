package datawave.next.retrieval;

import static datawave.query.iterator.QueryOptions.COMPOSITE_METADATA;
import static datawave.query.iterator.QueryOptions.DISALLOWLISTED_FIELDS;
import static datawave.query.iterator.QueryOptions.END_TIME;
import static datawave.query.iterator.QueryOptions.PROJECTION_FIELDS;
import static datawave.query.iterator.QueryOptions.QUERY;
import static datawave.query.iterator.QueryOptions.QUERY_MAPPING_COMPRESS;
import static datawave.query.iterator.QueryOptions.START_TIME;
import static datawave.query.iterator.QueryOptions.TYPE_METADATA;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.codec.binary.Base64;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import datawave.next.LongRange;
import datawave.query.composite.CompositeMetadata;
import datawave.query.iterator.QueryOptions;
import datawave.query.util.TypeMetadata;

/**
 * Similar to {@link QueryOptions}, a class that handles the busy work of parsing options to clean up the implementation of the {@link DocumentIterator}.
 */
public class DocumentIteratorOptions implements OptionDescriber {

    public static final String CANDIDATES = "candidates";

    // variables set via call to init
    protected SortedKeyValueIterator<Key,Value> source = null;
    protected Map<String,String> options = null;
    protected IteratorEnvironment env;

    // column families to exclude when retrieving documents
    protected final Collection<ByteSequence> excludeCFs = Lists.newArrayList(new ArrayByteSequence("tf"), new ArrayByteSequence("d"));

    // variables set from options
    protected String query;
    protected boolean compressedOptions = false;
    protected TypeMetadata typeMetadata;
    protected CompositeMetadata compositeMetadata;
    protected LongRange timeFilter;
    protected Set<String> includeFields = null;
    protected Set<String> excludeFields = null;
    protected final List<String> candidates = new ArrayList<>();

    //  @formatter:off
    protected static final Set<String> optionNames = Set.of(QUERY,
            QUERY_MAPPING_COMPRESS,
            START_TIME,
            END_TIME,
            TYPE_METADATA,
            COMPOSITE_METADATA,
            PROJECTION_FIELDS,
            DISALLOWLISTED_FIELDS,
            CANDIDATES);
    //  @formatter:on

    public void deepCopy(DocumentIteratorOptions other) {
        // source deep copied above
        this.options = other.options;
        this.env = other.env;

        this.query = other.query;
        this.compressedOptions = other.compressedOptions;
        this.typeMetadata = other.typeMetadata;
        this.compositeMetadata = other.compositeMetadata;
        this.timeFilter = other.timeFilter;
        this.includeFields = other.includeFields;
        this.excludeFields = other.excludeFields;
        this.candidates.addAll(other.candidates);
    }

    /**
     * Get the set of required option names. Useful when down-selecting from an existing set of options
     *
     * @return option names
     */
    public static Set<String> getRequiredOptionNames() {
        return optionNames;
    }

    @Override
    public IteratorOptions describeOptions() {
        Map<String,String> options = new HashMap<>();
        options.put(QUERY, "the query string");
        options.put(QUERY_MAPPING_COMPRESS, "true if the type metadata is base64 encoded");
        options.put(TYPE_METADATA, "TypeMetadata");
        options.put(COMPOSITE_METADATA, "CompositeMetadata");
        options.put(START_TIME, "the start time");
        options.put(END_TIME, "the end time");
        options.put(PROJECTION_FIELDS, "the set of fields to include");
        options.put(DISALLOWLISTED_FIELDS, "the set of fields to exclude");
        options.put(CANDIDATES, "the set of candidate record ids to fetch");
        return new IteratorOptions(getClass().getSimpleName(), "Retrieves documents", options, null);
    }

    @Override
    public boolean validateOptions(Map<String,String> options) {
        if (options.containsKey(QUERY)) {
            query = options.get(QUERY);
        } else {
            throw new RuntimeException("DocumentIterator requires a QUERY option");
        }

        if (options.containsKey(CANDIDATES)) {
            String option = options.get(CANDIDATES);
            candidates.addAll(Splitter.on(',').splitToList(option));
            // candidates are sorted to avoid expensive re-seeks
            Collections.sort(candidates);
        } else {
            throw new RuntimeException("BatchDocumentIterator requires CANDIDATES option");
        }

        if (options.containsKey(QUERY_MAPPING_COMPRESS)) {
            compressedOptions = Boolean.parseBoolean(options.get(QUERY_MAPPING_COMPRESS));
        }

        // Serialized version of a mapping from field name to DataType used
        if (options.containsKey(TYPE_METADATA)) {
            String option = options.get(TYPE_METADATA);
            try {
                if (compressedOptions) {
                    option = decompressOption(option, QueryOptions.UTF8);
                }

                this.typeMetadata = new TypeMetadata(option);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            throw new RuntimeException("Cannot execute query without TypeMetadata");
        }

        if (options.containsKey(COMPOSITE_METADATA)) {
            // no exception is thrown if this key does not exist because CompositeMetadata is not strictly
            // required. However, evaluation may be adversely affected.
            String option = options.get(COMPOSITE_METADATA);
            this.compositeMetadata = CompositeMetadata.fromBytes(java.util.Base64.getDecoder().decode(option));
        }

        if (options.containsKey(START_TIME) && options.containsKey(END_TIME)) {
            long start = Long.parseLong(options.get(START_TIME));
            long end = Long.parseLong(options.get(END_TIME));
            this.timeFilter = LongRange.of(start, end);
        } else {
            throw new RuntimeException("Query must have a time bound");
        }

        // include fields are optional
        if (options.containsKey(QueryOptions.PROJECTION_FIELDS)) {
            String option = options.get(QueryOptions.PROJECTION_FIELDS);
            // if the user requested everything with a star then leave include fields as null
            // this signifies that all fields are allowed
            if (!option.equals("*")) {
                includeFields = new HashSet<>();
                includeFields.addAll(Splitter.on(',').splitToList(option));
            }
        }

        // technically cannot have both include and exclude fields...but this iterator does not validate optional
        // options, that is the responsibility of the caller
        if (options.containsKey(QueryOptions.DISALLOWLISTED_FIELDS)) {
            String option = options.get(QueryOptions.DISALLOWLISTED_FIELDS);
            excludeFields = new HashSet<>();
            excludeFields.addAll(Splitter.on(',').splitToList(option));
        }

        return true;
    }

    /**
     * Direct lift from the QueryIterator
     *
     * @param buffer
     *            the data
     * @param characterSet
     *            the character set
     * @return the decompressed data
     * @throws IOException
     *             if there is a deserialization exception
     */
    protected String decompressOption(final String buffer, Charset characterSet) throws IOException {
        final byte[] inBase64 = Base64.decodeBase64(buffer.getBytes());

        ByteArrayInputStream byteInputStream = new ByteArrayInputStream(inBase64);
        GZIPInputStream gzipInputStream = new GZIPInputStream(byteInputStream);
        DataInputStream dataInputStream = new DataInputStream(gzipInputStream);

        final int length = dataInputStream.readInt();
        final byte[] dataBytes = new byte[length];
        dataInputStream.readFully(dataBytes, 0, length);

        dataInputStream.close();
        gzipInputStream.close();

        return new String(dataBytes, characterSet);
    }
}
