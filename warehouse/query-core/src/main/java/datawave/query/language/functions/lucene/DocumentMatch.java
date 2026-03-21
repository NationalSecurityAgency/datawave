package datawave.query.language.functions.lucene;

import java.text.MessageFormat;
import java.util.ArrayList;

import datawave.query.language.functions.QueryFunction;
import datawave.query.search.WildcardFieldedFilter;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;

/**
 * Lucene-language representation of {@code #DOCUMENT_MATCH(...)}.
 * <p>
 * This class exists in the Lucene query-language layer so the parser can recognize the function and carry it through the same fielded-filter machinery used by
 * other Lucene functions before the query is rendered into JEXL. The runtime semantics are still provided by {@code document:match(...)} in the evaluation
 * phase.
 */
@Deprecated
public class DocumentMatch extends LuceneQueryFunction {
    private static class DocumentMatchFilter extends WildcardFieldedFilter {
        private final String renderedQuery;

        DocumentMatchFilter(String selector) {
            super(true, WildcardFieldedFilter.BooleanType.AND);
            setField("document");
            setSelector(selector);
            this.renderedQuery = "document:" + selector;
            this.query = renderedQuery;
        }

        @Override
        public String toString() {
            return renderedQuery;
        }
    }

    public DocumentMatch() {
        super("DOCUMENT_MATCH", new ArrayList<>());
    }

    /**
     * Validates that {@code #DOCUMENT_MATCH(...)} received either one argument ({@code STRING}) or two arguments ({@code VIEWNAME, STRING}).
     *
     * @throws IllegalArgumentException
     *             if the function has no arguments or more than two arguments
     */
    @Override
    public void validate() throws IllegalArgumentException {
        if (this.parameterList == null || this.parameterList.isEmpty() || this.parameterList.size() > 2) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS, MessageFormat.format("{0}", this.name));
            throw new IllegalArgumentException(qe);
        }
    }

    /**
     * Initializes the Lucene-layer fielded-filter representation used during parsing.
     * <p>
     * The synthetic {@code document:match(...)} selector created here is a parser-level representation only; actual evaluation is deferred until the translated
     * JEXL query runs against candidate documents.
     *
     * @param parameterList
     *            parsed function arguments
     * @param depth
     *            function-node depth in the Lucene parse tree
     * @param parent
     *            parent query node
     * @throws IllegalArgumentException
     *             if initialization fails
     */
    @Override
    public void initialize(java.util.List<String> parameterList, int depth, org.apache.lucene.queryparser.flexible.core.nodes.QueryNode parent)
                    throws IllegalArgumentException {
        super.initialize(parameterList, depth, parent);
        this.fieldedFilter = new DocumentMatchFilter(buildSelector());
    }

    /**
     * Builds the parser-layer selector text {@code match(...)} from the raw Lucene arguments.
     *
     * @return selector text used by the synthetic fielded filter
     */
    private String buildSelector() {
        StringBuilder sb = new StringBuilder();
        sb.append("match(");
        for (int i = 0; i < parameterList.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(parameterList.get(i));
        }
        sb.append(")");
        return sb.toString();
    }

    /**
     * @return a fresh function instance for parser duplication
     */
    @Override
    public QueryFunction duplicate() {
        return new DocumentMatch();
    }
}
