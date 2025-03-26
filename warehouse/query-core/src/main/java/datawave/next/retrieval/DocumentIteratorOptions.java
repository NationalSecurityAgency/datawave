package datawave.next.retrieval;

import static datawave.query.iterator.QueryOptions.QUERY_MAPPING_COMPRESS;
import static datawave.query.iterator.QueryOptions.TYPE_METADATA;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
    protected final List<String> candidates = new ArrayList<>();

    @Override
    public IteratorOptions describeOptions() {
        return null;
    }

    @Override
    public boolean validateOptions(Map<String,String> options) {
        if (options.containsKey(QueryOptions.QUERY)) {
            query = options.get(QueryOptions.QUERY);
        } else {
            throw new RuntimeException("DocumentIterator requires a query option");
        }

        if (options.containsKey(CANDIDATES)) {
            String opt = options.get(CANDIDATES);
            candidates.addAll(Splitter.on(',').splitToList(opt));
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

        return true;
    }

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
