package datawave.query.function;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.collect.Maps;

import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Document;
import datawave.query.attributes.DocumentKey;
import datawave.query.jexl.functions.DocumentFunctions;
import datawave.query.util.Tuple3;
import datawave.query.util.Tuples;

/**
 * Builds a {@link DocumentMatchContext} close to evaluation time and attaches it to the side-channel map used for JEXL context population.
 */
public class DocumentMatchContextFunction implements Function<Tuple3<Key,Document,Map<String,Object>>,Tuple3<Key,Document,Map<String,Object>>> {
    private static final Logger log = Logger.getLogger(DocumentMatchContextFunction.class);
    private final DocumentMatchConfig config;

    /**
     * Creates a context-populating function from the supplied document-match configuration.
     *
     * @param config
     *            document-match configuration
     */
    public DocumentMatchContextFunction(DocumentMatchConfig config) {
        this.config = config;
    }

    @Override
    public Tuple3<Key,Document,Map<String,Object>> apply(Tuple3<Key,Document,Map<String,Object>> from) {
        try {
            Set<Key> documentKeys = getDocumentKeys(from.first(), from.second());
            if (log.isDebugEnabled()) {
                log.debug("Collecting document-match context for tuple key " + from.first() + " using document keys " + documentKeys);
            }

            List<Entry<Key,Value>> dEntries = collectDocumentColumnAttributes(documentKeys);
            DocumentMatchContext context = DocumentMatchContext.from(dEntries, config.getTimeFilter(), config.getLimits());
            if (log.isDebugEnabled()) {
                log.debug("Collected " + dEntries.size() + " d-column entries for tuple key " + from.first());
            }

            Map<String,Object> map = from.third().isEmpty() ? new HashMap<>() : new HashMap<>(from.third());
            map.put(DocumentFunctions.DOCUMENT_MATCH_CONTEXT_JEXL_VARIABLE_NAME, context);
            return Tuples.tuple(from.first(), from.second(), map);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to collect document-match context for " + from.first(), e);
        }
    }

    private List<Entry<Key,Value>> collectDocumentColumnAttributes(Set<Key> documentKeys) throws IOException {
        List<Entry<Key,Value>> documentColumns = new ArrayList<>();
        for (Key documentKey : documentKeys) {
            collectDocumentColumnAttributes(documentKey, documentColumns);
        }
        return documentColumns;
    }

    private void collectDocumentColumnAttributes(Key documentKey, List<Entry<Key,Value>> documentColumns) throws IOException {
        String row = documentKey.getRow().toString();
        String datatypeAndUid = documentKey.getColumnFamily().toString();
        Key startKey = new Key(row, "d", datatypeAndUid + '\0');
        Key endKey = new Key(row, "d", datatypeAndUid + '\uffff');
        Range documentColumnRange = new Range(startKey, true, endKey, false);
        if (log.isDebugEnabled()) {
            log.debug("Seeking d-column range " + documentColumnRange + " for document key " + documentKey);
        }

        config.getSource().seek(documentColumnRange, Collections.emptyList(), false);

        while (config.getSource().hasTop() && isDocumentColumn(config.getSource().getTopKey(), documentKey)) {
            if (log.isDebugEnabled()) {
                log.debug("Collected d-column entry " + config.getSource().getTopKey() + " for document key " + documentKey);
            }
            documentColumns.add(Maps.immutableEntry(config.getSource().getTopKey(), config.getSource().getTopValue()));
            config.getSource().next();
        }

        if (log.isDebugEnabled()) {
            log.debug("Finished d-column scan for document key " + documentKey + "; next top key is "
                            + (config.getSource().hasTop() ? config.getSource().getTopKey() : "<none>"));
        }
    }

    private Set<Key> getDocumentKeys(Key tupleKey, Document document) {
        Set<Key> docKeys = new HashSet<>((config.isTld()) ? 4 : 1);
        Attribute<?> docKeyAttr = document.get(Document.DOCKEY_FIELD_NAME);
        if (docKeyAttr == null) {
            docKeys.add(tupleKey);
            return docKeys;
        }

        if (docKeyAttr instanceof DocumentKey) {
            docKeys.add(((DocumentKey) docKeyAttr).getDocKey());
        } else if (docKeyAttr instanceof Attributes) {
            for (Attribute<?> docKey : ((Attributes) docKeyAttr).getAttributes()) {
                if (docKey instanceof DocumentKey) {
                    docKeys.add(((DocumentKey) docKey).getDocKey());
                } else {
                    throw new IllegalStateException("Unexpected sub-Attribute type for " + Document.DOCKEY_FIELD_NAME + ": " + docKey.getClass());
                }
            }
        } else {
            throw new IllegalStateException("Unexpected Attribute type for " + Document.DOCKEY_FIELD_NAME + ": " + docKeyAttr.getClass());
        }

        if (docKeys.isEmpty()) {
            docKeys.add(tupleKey);
        }
        return docKeys;
    }

    /**
     * Determines whether a scanned key is a {@code d}-column for the supplied document key.
     * <p>
     * The comparison intentionally checks the scanned key's column qualifier against the document key's column family. For event keys, the column family is
     * {@code datatype\0uid}, while {@code d}-column qualifiers are laid out as {@code datatype\0uid\0view}. Matching on this prefix ensures that the collected
     * {@code d}-column belongs to the same document identity as the event key.
     *
     * @param documentContentKey
     *            scanned 'd' column shard-table key
     * @param documentKey
     *            event or document key whose {@code datatype\0uid} identifies the document
     * @return {@code true} if the scanned key is a matching {@code d}-column entry for the document
     */
    private boolean isDocumentColumn(Key documentContentKey, Key documentKey) {
        // A document key's column family is datatype\0uid, and a d-column qualifier begins with that same datatype\0uid
        // followed by \0view. This prefix comparison ties the d-column back to the document represented by the event key.
        return documentContentKey.getColumnFamilyData().length() == 1 && documentContentKey.getColumnFamilyData().byteAt(0) == 'd'
                        && documentContentKey.getRow().equals(documentKey.getRow())
                        && documentContentKey.getColumnQualifier().toString().startsWith(documentKey.getColumnFamily().toString() + '\0');
    }
}
