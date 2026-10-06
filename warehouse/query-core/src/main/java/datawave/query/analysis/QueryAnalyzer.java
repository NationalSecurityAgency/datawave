package datawave.query.analysis;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.jexl3.parser.ASTJexlScript;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.language.parser.jexl.LuceneToJexlQueryParser;

/**
 * Offline, metadata-free categorization of query text. Lucene is converted using DataWave's parser before analysis. No query is executed or planned by this
 * class. A successful analysis does not establish that a query is executable against a particular schema or index.
 *
 * <p>
 * Clusters are exact structural families: string and numeric values are replaced by the same placeholder, and associative AND/OR children are sorted. Fields
 * (including case and grouping context), operators, argument order, null/boolean literals, and query property markers are retained. Cluster membership does not
 * imply equivalent results or execution cost. Duplicate inputs retain their individual positions. Syntax is explicit because guessing can silently reinterpret
 * malformed JEXL as Lucene.
 *
 * <pre>
 * QueryAnalyzer analyzer = new QueryAnalyzer();
 * AnalysisReport report = analyzer.analyze(Arrays.asList(new QueryInput("NAME:alice", Syntax.LUCENE), new QueryInput("NAME == 'bob'", Syntax.JEXL)));
 * // Both inputs belong to one structural cluster, with category EQUALITY.
 * </pre>
 */
public final class QueryAnalyzer {
    public enum Syntax {
        LUCENE, JEXL
    }

    /** Multiple categories can apply to one query; these describe syntax, not planner decisions. */
    public enum Category {
        EQUALITY, RANGE, REGEX, NEGATION, CONJUNCTION, DISJUNCTION, FUNCTION, CONTENT, GEOSPATIAL, NULL_CHECK, UNFIELDED, PROPERTY_MARKER, OTHER
    }

    public enum Status {
        SUCCESS, INVALID, UNSUPPORTED
    }

    private final LuceneToJexlQueryParser luceneParser;

    public QueryAnalyzer() {
        this(new LuceneToJexlQueryParser());
    }

    /**
     * Use a deployment-configured parser when tokenization, allowed fields, or functions differ from the defaults. The caller owns parser configuration and
     * must not mutate it during analysis. Calls through this analyzer are serialized because a configured parser need not be thread safe.
     *
     * @param luceneParser
     *            the Lucene-to-JEXL parser
     */
    public QueryAnalyzer(LuceneToJexlQueryParser luceneParser) {
        this.luceneParser = Objects.requireNonNull(luceneParser, "luceneParser");
    }

    /** Analyze a list in one syntax. Null or blank text produces an INVALID entry. */
    public AnalysisReport analyze(List<String> queries, Syntax syntax) {
        Objects.requireNonNull(queries, "queries");
        Objects.requireNonNull(syntax, "syntax");
        List<QueryInput> inputs = new ArrayList<>(queries.size());
        for (String query : queries) {
            inputs.add(new QueryInput(query, syntax));
        }
        return analyze(inputs);
    }

    /** Analyze a mixed-syntax batch, retaining input order and isolating errors to individual entries. */
    public synchronized AnalysisReport analyze(List<QueryInput> queries) {
        Objects.requireNonNull(queries, "queries");
        List<QueryAnalysis> results = new ArrayList<>(queries.size());
        for (int index = 0; index < queries.size(); index++) {
            results.add(analyze(index, queries.get(index)));
        }
        return new AnalysisReport(results);
    }

    /** Summarize an existing clustering result without analyzing queries or comparing fingerprints again. */
    public QueryMinimizationReport summarizeMinimization(QueryClusterer.Result result) {
        return QueryMinimizationReport.from(Objects.requireNonNull(result, "result"));
    }

    /** Capture immutable clustering and selection statistics without advancing the mutable cursor. */
    public QueryMinimizationReport summarizeMinimization(QueryWorkloadSelector.Cursor cursor) {
        return QueryMinimizationReport.from(Objects.requireNonNull(cursor, "cursor"));
    }

    private QueryAnalysis analyze(int index, QueryInput input) {
        if (input == null || input.getSyntax() == null || input.getQuery() == null || input.getQuery().trim().isEmpty()) {
            return QueryAnalysis.failure(index, input, Status.INVALID, "Query text and syntax are required");
        }
        String jexl;
        ASTJexlScript script;
        try {
            jexl = input.getSyntax() == Syntax.LUCENE ? luceneParser.parse(input.getQuery()).getOriginalQuery() : input.getQuery();
            script = JexlASTHelper.parseJexlQuery(jexl);
        } catch (datawave.query.language.parser.ParseException | org.apache.commons.jexl3.parser.ParseException | IllegalArgumentException e) {
            return QueryAnalysis.failure(index, input, Status.INVALID, "Unable to parse " + input.getSyntax() + ": " + e.getMessage());
        }
        try {
            QueryShape shape = new QueryShape();
            String signature = shape.analyze(script);
            QueryFingerprint fingerprint = new QueryFingerprintBuilder().build(script);
            return new QueryAnalysis(index, input, Status.SUCCESS, jexl, signature, shape.categories, shape.fields, shape.functions, null, fingerprint);
        } catch (UnsupportedOperationException e) {
            return QueryAnalysis.failure(index, input, Status.UNSUPPORTED, e.getMessage());
        }
    }

    public static final class QueryInput {
        private final String query;
        private final Syntax syntax;

        public QueryInput(String query, Syntax syntax) {
            this.query = query;
            this.syntax = syntax;
        }

        public String getQuery() {
            return query;
        }

        public Syntax getSyntax() {
            return syntax;
        }
    }

    public static final class QueryAnalysis {
        private final int index;
        private final QueryInput input;
        private final Status status;
        private final String jexl;
        private final String signature;
        private final Set<Category> categories;
        private final Set<String> fields;
        private final Set<String> functions;
        private final String error;
        private final QueryFingerprint fingerprint;

        private QueryAnalysis(int index, QueryInput input, Status status, String jexl, String signature, Set<Category> categories, Set<String> fields,
                        Set<String> functions, String error, QueryFingerprint fingerprint) {
            this.index = index;
            this.input = input;
            this.status = status;
            this.jexl = jexl;
            this.signature = signature;
            EnumSet<Category> categoryCopy = EnumSet.noneOf(Category.class);
            categoryCopy.addAll(categories);
            this.categories = Collections.unmodifiableSet(categoryCopy);
            this.fields = Collections.unmodifiableSet(new TreeSet<>(fields));
            this.functions = Collections.unmodifiableSet(new TreeSet<>(functions));
            this.error = error;
            this.fingerprint = fingerprint;
        }

        private static QueryAnalysis failure(int index, QueryInput input, Status status, String error) {
            return new QueryAnalysis(index, input, status, null, null, Collections.emptySet(), Collections.emptySet(), Collections.emptySet(), error, null);
        }

        public int getIndex() {
            return index;
        }

        public QueryInput getInput() {
            return input;
        }

        public Status getStatus() {
            return status;
        }

        /** Original JEXL or the parser's Lucene translation, retaining values for downstream use. Null on failure. */
        public String getJexl() {
            return jexl;
        }

        /** Value-independent structural key. Null on failure; never use this key as an executable query. */
        public String getSignature() {
            return signature;
        }

        /** Rich planning-sensitive features, or null on failure. Legacy exact signatures and clusters remain unchanged. */
        public QueryFingerprint getFingerprint() {
            return fingerprint;
        }

        public Set<Category> getCategories() {
            return categories;
        }

        /** Exact field identifiers; excludes _ANYFIELD_ and content's termOffsetMap variable. */
        public Set<String> getFields() {
            return fields;
        }

        public Set<String> getFunctions() {
            return functions;
        }

        public String getError() {
            return error;
        }
    }

    public static final class AnalysisReport {
        private final List<QueryAnalysis> queries;
        private final Map<String,List<QueryAnalysis>> clusters;

        private AnalysisReport(List<QueryAnalysis> queries) {
            this.queries = Collections.unmodifiableList(new ArrayList<>(queries));
            Map<String,List<QueryAnalysis>> grouped = new TreeMap<>();
            for (QueryAnalysis query : queries) {
                if (query.getStatus() == Status.SUCCESS) {
                    grouped.computeIfAbsent(query.getSignature(), key -> new ArrayList<>()).add(query);
                }
            }
            grouped.replaceAll((key, values) -> Collections.unmodifiableList(values));
            clusters = Collections.unmodifiableMap(grouped);
        }

        public List<QueryAnalysis> getQueries() {
            return queries;
        }

        /** Successful queries grouped by signature, in signature order; members retain input order. Failed queries are excluded. */
        public Map<String,List<QueryAnalysis>> getClusters() {
            return clusters;
        }
    }
}
