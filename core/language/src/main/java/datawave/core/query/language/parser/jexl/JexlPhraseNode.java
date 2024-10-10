package datawave.core.query.language.parser.jexl;

import java.util.ArrayList;
import java.util.List;

public class JexlPhraseNode extends JexlNode {
    private String field = null;
    private List<String> wordList = new ArrayList<>();

    public JexlPhraseNode(String field, List<String> wordList) {
        super(new ArrayList<>());
        this.field = field;
        this.wordList = wordList;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        if (this.field == null || this.field.isEmpty()) {
            sb.append("content:phrase(termOffsetMap");
        } else {
            sb.append("content:phrase(");
            sb.append(this.field);
            sb.append(", termOffsetMap");
        }

        for (String s : this.wordList) {
            sb.append(", ");
            sb.append("'");
            sb.append(jexlEscapeSelector(s));
            sb.append("'");
        }
        // Matches function argument list
        sb.append(")");

        return sb.toString();
    }

    public String getField() {
        return this.field;
    }

    public List<String> getWordList() {
        return this.wordList;
    }
}
