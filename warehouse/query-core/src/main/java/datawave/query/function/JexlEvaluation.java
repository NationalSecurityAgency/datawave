package datawave.query.function;

import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.jexl3.JexlArithmetic;
import org.apache.commons.jexl3.internal.DatawaveJexlScript;
import org.apache.commons.jexl3.internal.Script;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.log4j.Logger;

import com.google.common.base.Predicate;

import datawave.query.Constants;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Content;
import datawave.query.attributes.Document;
import datawave.query.attributes.ValueTuple;
import datawave.query.jexl.ArithmeticJexlEngines;
import datawave.query.jexl.DatawaveJexlContext;
import datawave.query.jexl.DatawaveJexlEngine;
import datawave.query.jexl.DefaultArithmetic;
import datawave.query.jexl.DelayedNonEventIndexContext;
import datawave.query.jexl.HitListArithmetic;
import datawave.query.postprocessing.tf.PhraseIndexes;
import datawave.query.postprocessing.tf.TermOffsetMap;
import datawave.query.transformer.ExcerptTransform;
import datawave.query.util.Tuple3;

public class JexlEvaluation implements Predicate<Tuple3<Key,Document,DatawaveJexlContext>> {
    private static final Logger log = Logger.getLogger(JexlEvaluation.class);

    public static final String HIT_TERM_FIELD = "HIT_TERM";

    private String query;
    private JexlArithmetic arithmetic;
    private DatawaveJexlEngine engine;

    // do we need to gather phrase offsets
    private boolean gatherPhraseOffsets = false;

    // The set of fields for which we should gather phrase offsets for.
    private Set<String> phraseOffsetFields;

    /**
     * Compiled and flattened jexl script
     */
    protected DatawaveJexlScript script;

    public JexlEvaluation(String query) {
        this(query, new DefaultArithmetic());
    }

    public JexlEvaluation(String query, JexlArithmetic arithmetic) {
        this.query = query;
        this.arithmetic = arithmetic;

        // Get a JexlEngine initialized with the correct JexlArithmetic for this Document
        this.engine = ArithmeticJexlEngines.getEngine(arithmetic);

        // Evaluate the JexlContext against the Script
        this.script = DatawaveJexlScript.create((Script) this.engine.createScript(this.query));
    }

    public JexlArithmetic getArithmetic() {
        return arithmetic;
    }

    public DatawaveJexlEngine getEngine() {
        return engine;
    }

    public ASTJexlScript parse(String expression) {
        return engine.parse(expression);
    }

    public boolean isMatched(Object o) {
        return ArithmeticJexlEngines.isMatched(o);
    }

    @Override
    public boolean apply(Tuple3<Key,Document,DatawaveJexlContext> input) {

        // setup the term offset map to gather phrase indexes if requested.
        TermOffsetMap termOffsetMap = (TermOffsetMap) input.third().get(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME);
        if (termOffsetMap != null && isGatherPhraseOffsets() && arithmetic instanceof HitListArithmetic) {
            termOffsetMap.setGatherPhraseOffsets(true);
            termOffsetMap.setExcerptFields(phraseOffsetFields);
        }

        // now evaluate
        Object o = script.execute(input.third());
        if (log.isTraceEnabled()) {
            log.trace("Evaluation of " + input.first() + " with " + query + " against " + input.third() + " returned " + o);
        }

        boolean matched = isMatched(o);

        // Add delayed info to document
        if (matched && input.third() instanceof DelayedNonEventIndexContext) {
            ((DelayedNonEventIndexContext) input.third()).populateDocument(input.second());
        }

        if (arithmetic instanceof HitListArithmetic) {
            HitListArithmetic hitListArithmetic = (HitListArithmetic) arithmetic;
            if (matched) {
                Document document = input.second();

                Attributes attributes = new Attributes(input.second().isToKeep());

                for (ValueTuple hitTuple : hitListArithmetic.getHitTuples()) {

                    ColumnVisibility cv = null;
                    Key metadata = null;
                    String term = hitTuple.getFieldName() + ':' + hitTuple.getValue();

                    if (hitTuple.getSource() != null) {
                        cv = hitTuple.getSource().getColumnVisibility();
                        metadata = hitTuple.getSource().getMetadata();
                    }

                    // fall back to extracting column visibility from document
                    if (cv == null) {
                        // get the visibility for the record with this hit
                        cv = HitListArithmetic.getColumnVisibilityForHit(document, term);
                        // if no visibility computed, then there were no hits that match fields still in the document......
                    }
                    // fall back to the metadata from document
                    if (metadata == null) {
                        metadata = document.getMetadata();
                    }

                    if (cv != null) {
                        // unused
                        long timestamp = document.getTimestamp(); // will force an update to make the metadata valid
                        Content content = new Content(term, metadata, document.isToKeep());
                        content.setColumnVisibility(cv);
                        attributes.add(content);
                    }
                }
                if (attributes.size() > 0) {
                    document.put(HIT_TERM_FIELD, attributes);
                }

                // Put the phrase indexes into the document so that we can add phrase excerpts if desired later.
                if (termOffsetMap != null) {
                    PhraseIndexes phraseIndexes = termOffsetMap.getPhraseIndexes();
                    if (phraseIndexes != null) {
                        Content phraseIndexesAttribute = new Content(phraseIndexes.toString(), document.getMetadata(), false);
                        document.put(ExcerptTransform.PHRASE_INDEXES_ATTRIBUTE, phraseIndexesAttribute);
                        if (log.isTraceEnabled()) {
                            log.trace("Added phrase-indexes " + phraseIndexes + " as attribute " + ExcerptTransform.PHRASE_INDEXES_ATTRIBUTE + " to document "
                                            + document.getMetadata());
                        }
                    }
                }
            }
            hitListArithmetic.clear();
        }
        return matched;
    }

    public boolean isGatherPhraseOffsets() {
        return gatherPhraseOffsets;
    }

    public void setGatherPhraseOffsets(boolean gatherPhraseOffsets) {
        this.gatherPhraseOffsets = gatherPhraseOffsets;
    }

    public Set<String> getPhraseOffsetFields() {
        return phraseOffsetFields;
    }

    public void setPhraseOffsetFields(Set<String> phraseOffsetFields) {
        this.phraseOffsetFields = phraseOffsetFields;
    }
}
