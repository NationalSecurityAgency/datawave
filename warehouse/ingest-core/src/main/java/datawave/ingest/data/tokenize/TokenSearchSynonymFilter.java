package datawave.ingest.data.tokenize;

import java.io.IOException;
import java.util.Collection;
import java.util.PriorityQueue;
import java.util.Queue;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;

/** Wraps DefaultTokenSearch and exposes it as a Lucene TokenFilter. Not thread-safe */
public class TokenSearchSynonymFilter extends TokenFilter {

    /*
     * Lucene attributes used to hold current tokenizer state and share it amongst participates in the TokenStream pipleline.
     */
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);
    private final PositionLengthAttribute posLenAtt = addAttribute(PositionLengthAttribute.class);
    private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);
    private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);

    private final TokenSearch searchUtil;
    boolean synonymPositionsEnabled = false;

    /** tokenizer state: output buffer for terms and synonyms */
    private final Queue<OutputTuple> output = new PriorityQueue<>(24);
    private final String[] zw = {"", ""};

    public TokenSearchSynonymFilter(TokenStream input, TokenSearch searchUtil) {
        super(input);
        this.searchUtil = searchUtil;
    }

    public boolean isSynonymPositionsEnabled() {
        return synonymPositionsEnabled;
    }

    /**
     * Enable synonym positions. If enabled, each synonum is exactly one term wide, and the term they originate from is set to have a length equal to the number
     * of synonyms generated for it. Each synonym receives a position increment of one.
     *
     * If false, each synonym has a position increment and length of zero, and the originating term has a position offset and length of one.
     *
     * We currently don't store position length in the index, so this should not be turned on until we do, and phrase search is capable of taking advantage of
     * it. Otherwise, we'll mess up the adjacency of synonym terms.
     *
     * @param synonymPositionsEnabled
     *            flag to determine synonym positions function
     */
    public void setSynonymPositionsEnabled(boolean synonymPositionsEnabled) {
        this.synonymPositionsEnabled = synonymPositionsEnabled;
    }

    @Override
    public void reset() throws IOException {
        super.reset();
    }

    @Override
    public final boolean incrementToken() throws IOException {
        OutputTuple outputTuple;

        while (true) {
            if ((outputTuple = output.poll()) != null) { // return buffered output state
                restoreTuple(outputTuple);
                return true;
            } else if (!input.incrementToken()) { // no more tokens available from source
                return false;
            }

            State baseState = captureState();

            int synonymCount = generateSynonyms();

            restoreState(baseState);
            if (synonymCount > 0 && synonymPositionsEnabled) {
                int currentLength = posLenAtt.getPositionLength();
                posLenAtt.setPositionLength(Math.max(synonymCount, currentLength));
            }
            return true;
        }
    }

    protected int generateSynonyms() {
        String type = typeAtt.type();

        zw[0] = termAtt.toString();
        zw[1] = ""; // placeholder for zone, unused in DefaultTokenSearch

        Collection<String> synonyms = searchUtil.getSynonyms(zw, type, false);
        int position = 0;

        // create output type for synonyms
        if (type.startsWith("<") && type.endsWith(">")) {
            type = "<" + type.substring(1, type.length() - 1) + "_SYNONYM>";
        } else {
            type = type + "_SYNONYM";
        }

        // TODO: it would be nice to capture proper character offset
        // information here, but that would require a significant change in
        // operation of DefaultTokenSearch. For now, the offset of each synonym
        // is that of the term from which it is derived.

        for (String synonym : synonyms) {
            char[] cc = synonym.toCharArray();
            termAtt.copyBuffer(cc, 0, cc.length);
            typeAtt.setType(type);

            posLenAtt.setPositionLength(1);

            if (synonymPositionsEnabled) {
                // first synonym co-occurs with original term
                posIncrAtt.setPositionIncrement(position == 0 ? 0 : 1);
            } else {
                posIncrAtt.setPositionIncrement(0);
                posLenAtt.setPositionLength(1);
            }

            captureTuple(position++);
        }
        return position;
    }

    /**
     * Capture information about the tuple to the output buffer
     *
     * @param position
     *            position in the buffer
     */
    protected void captureTuple(int position) {
        output.add(new OutputTuple(termAtt.toString(), position, offsetAtt.startOffset(), offsetAtt.endOffset() - offsetAtt.startOffset(), captureState()));
    }

    /**
     * Restore captured state to the Lucene attributes
     *
     * @param tuple
     *            tuple to restore
     * @return position of the tuple
     */
    protected int restoreTuple(OutputTuple tuple) {
        restoreState(tuple.state);
        return tuple.position;
    }

    /**
     * Segment state captured in the output buffer, member variables are used for ordering.
     */
    private static final class OutputTuple implements Comparable<OutputTuple> {
        private final String term;
        private final int position;
        private final int offset;
        private final int length;

        State state;

        private OutputTuple(String term, int position, int offset, int length, State s) {
            this.term = term;
            this.position = position;
            this.offset = offset;
            this.length = length;
            this.state = s;
        }

        @Override
        public int compareTo(OutputTuple other) {
            int cmp = this.position - other.position;
            if (cmp != 0) {
                return cmp;
            }

            cmp = this.offset - other.offset;
            if (cmp != 0) {
                return cmp;
            }

            cmp = other.length - this.length;
            if (cmp != 0) {
                return cmp;
            }

            if (this.term == null && other.term == null) {
                return 0;
            } else if (this.term == null) {
                return 1;
            } else if (other.term == null) {
                return -1;
            } else {
                return this.term.compareTo(other.term);
            }
        }

        @Override
        public boolean equals(Object thatObject) {
            if (thatObject == null || !(thatObject instanceof OutputTuple)) {
                return false;
            }

            OutputTuple that = (OutputTuple) thatObject;

            if (this.position + this.offset + this.length != that.position + that.position + that.length) {
                return false;
            }

            if (this.term != null && that.term != null) {
                return this.term.equals(that.term);
            }

            if (this.term == null && that.term == null) {
                return true;
            }

            return false;
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder().append(term).append(position).append(offset).append(length).hashCode();
        }

        @Override
        public String toString() {
            return "OutputTuple [term=" + term + ", position=" + position + ", offset=" + offset + ", length=" + length + "]";
        }
    }
}
