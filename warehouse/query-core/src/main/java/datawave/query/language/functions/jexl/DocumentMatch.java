package datawave.query.language.functions.jexl;

import java.text.MessageFormat;
import java.util.ArrayList;

import datawave.query.language.functions.QueryFunction;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;

/**
 * JEXL-language representation of {@code document:match(...)}.
 * <p>
 * This function is produced by the query-language layer after parsing or after Lucene-to-JEXL translation. It validates the supported one-argument and
 * two-argument forms and renders the canonical JEXL syntax consumed by the runtime query planner.
 */
public class DocumentMatch extends JexlQueryFunction {

    public static final String DOCUMENT_MATCH_FUNCTION = "DOCUMENT_MATCH";
    public static final String DOCUMENT_FIELD = "document";
    public static final String DOCUMENT_NAMESPACE = DOCUMENT_FIELD + ":";
    public static final String MATCH_FUNCTION = "match";

    public DocumentMatch() {
        super(DOCUMENT_MATCH_FUNCTION, new ArrayList<>());
    }

    /**
     * Validates that {@code document:match(...)} received either one argument ({@code STRING}) or two arguments ({@code VIEWNAME, STRING}).
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
     * Renders the canonical JEXL form {@code document:match(...)} with escaped arguments.
     *
     * @return JEXL representation of this function
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(DOCUMENT_NAMESPACE + MATCH_FUNCTION + "(");
        for (int i = 0; i < parameterList.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(escapeString(parameterList.get(i)));
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
