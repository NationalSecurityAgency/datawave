package datawave.query.transformer;

import datawave.query.attributes.Attributes;
import datawave.query.attributes.Content;
import datawave.query.attributes.Document;
import datawave.query.attributes.ExcerptFields;
import datawave.query.iterator.logic.TermFrequencyExcerptIterator;
import datawave.query.postprocessing.tf.PhraseIndexes;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.javatuples.Pair;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class ExcerptTransform extends DocumentTransform.DefaultDocumentTransform {
    
    private static final Logger log = Logger.getLogger(ExcerptTransform.class);
    
    public static final String PHRASE_INDEXES_ATTRIBUTE = "PHRASE_INDEXES_ATTRIBUTE";
    public static final String HIT_EXCERPTS = "HIT_EXCERPTS";
    
    private final ExcerptFields excerptFields;
    private final IteratorEnvironment env;
    private final SortedKeyValueIterator<Key,Value> source;
    
    public ExcerptTransform(ExcerptFields excerptFields, IteratorEnvironment env, SortedKeyValueIterator<Key,Value> source) {
        this.excerptFields = excerptFields;
        this.env = env;
        this.source = source;
    }
    
    @Nullable
    @Override
    public Entry<Key,Document> apply(@Nullable Entry<Key,Document> entry) {
        if (entry != null) {
            Document document = entry.getValue();
            // Do not bother adding excerpts to transient documents.
            if (document.isToKeep()) {
                PhraseIndexes phraseIndexes = getPhraseIndexes(document);
                if (phraseIndexes != null) {
                    Set<String> excerpts = getExcerpts(phraseIndexes, document);
                    addExcerptsToDocument(excerpts, document);
                } else {
                    if (log.isTraceEnabled()) {
                        log.trace("Phrase indexes were not added to document " + document.getMetadata() + ", skipping");
                    }
                }
            }
        }
        return entry;
    }
    
    /**
     * Retrieve the phrase indexes from the {@value #PHRASE_INDEXES_ATTRIBUTE} attribute in the document.
     * 
     * @param document
     *            the document
     * @return the phrase indexes
     */
    private PhraseIndexes getPhraseIndexes(Document document) {
        Content content = (Content) document.get(PHRASE_INDEXES_ATTRIBUTE);
        return PhraseIndexes.from(content.getContent());
    }
    
    /**
     * Add the excerpts to the document as part of {@value #HIT_EXCERPTS}.
     * 
     * @param excerpts
     *            the excerpts to add
     * @param document
     *            the document
     */
    private void addExcerptsToDocument(Set<String> excerpts, Document document) {
        Attributes attributes = new Attributes(true);
        for (String excerpt : excerpts) {
            Content content = new Content(excerpt, document.getMetadata(), true);
            attributes.add(content);
        }
        document.put(HIT_EXCERPTS, attributes);
    }
    
    /**
     * Get the excerpts.
     * 
     * @param phraseIndexes
     *            the pre-identified phrase offsets
     * @return the excerpts
     */
    private Set<String> getExcerpts(PhraseIndexes phraseIndexes, Document document) {
        phraseIndexes = getOffsetPhraseIndexes(phraseIndexes);
        if (phraseIndexes.isEmpty()) {
            return Collections.emptySet();
        }
        
        // Construct the required range for this document.
        Key metadata = document.getMetadata();
        Key startKey = new Key(metadata.getRow(), new Text("datatype\0uid"));
        Key endKey = startKey.followingKey(PartialKey.ROW_COLFAM);
        Range range = new Range(startKey, true, endKey, false);
        
        // Fetch the excerpts.
        Set<String> excerpts = new HashSet<>();
        for (String field : phraseIndexes.getFields()) {
            Collection<Pair<Integer,Integer>> indexes = phraseIndexes.getIndices(field);
            for (Pair<Integer,Integer> indexPair : indexes) {
                String excerpt = getExcerpt(field, indexPair.getValue0(), indexPair.getValue1(), range);
                excerpts.add(excerpt);
            }
        }
        return excerpts;
    }
    
    private String getExcerpt(String field, int start, int end, Range range) {
        Map<String,String> options = new HashMap<>();
        options.put(TermFrequencyExcerptIterator.FIELD_NAME, field);
        options.put(TermFrequencyExcerptIterator.START_OFFSET, String.valueOf(start));
        options.put(TermFrequencyExcerptIterator.END_OFFSET, String.valueOf(end));
        TermFrequencyExcerptIterator iterator = new TermFrequencyExcerptIterator();
        try {
            iterator.init(source, options, env);
            iterator.seek(range, Collections.emptyList(), false);
            if (iterator.hasTop()) {
                iterator.getTopKey();
            }
            // TODO - seek to phrase
        } catch (IOException e) {
            throw new RuntimeException("Failed to scan for excerpts", e);
        }
        return null;
    }
    
    /**
     * Returned a filtered {@link PhraseIndexes} that only contains the fields for which excerpts are desired, with the indexes offset by the specified excerpt
     * offset.
     * 
     * @param phraseIndexes
     *            the original phrase indexes
     * @return the filtered, offset phrase indexes
     */
    private PhraseIndexes getOffsetPhraseIndexes(PhraseIndexes phraseIndexes) {
        PhraseIndexes offsetPhraseIndexes = new PhraseIndexes();
        for (String field : excerptFields.getFields()) {
            // Filter out phrases that are not in desired fields.
            if (phraseIndexes.containsField(field)) {
                Collection<Pair<Integer,Integer>> indexes = phraseIndexes.getIndices(field);
                int offset = excerptFields.getOffset(field);
                // Ensure the offset is modified to encompass the target excerpt range.
                for (Pair<Integer,Integer> indexPair : indexes) {
                    int start = indexPair.getValue0() - offset;
                    int end = indexPair.getValue1() + offset;
                    offsetPhraseIndexes.addIndexPair(field, start, end);
                }
            }
        }
        return offsetPhraseIndexes;
    }
    
    /**
     * Add phrase excerpts to the documents from the given iterator.
     * 
     * @param in
     *            the iterator source
     * @return an iterator that will supply the enriched documents
     */
    public Iterator<Entry<Key,Document>> getIterator(final Iterator<Entry<Key,Document>> in) {
        return new Iterator<Entry<Key,Document>>() {
            
            Entry<Key,Document> next;
            
            @Override
            public boolean hasNext() {
                if (in.hasNext()) {
                    next = ExcerptTransform.this.apply(in.next());
                }
                return next != null;
            }
            
            @Override
            public Entry<Key,Document> next() {
                return next;
            }
        };
    }
}
