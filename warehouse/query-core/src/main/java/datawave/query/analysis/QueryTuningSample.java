package datawave.query.analysis;

import java.util.Objects;

import datawave.query.analysis.QueryAnalyzer.QueryInput;
import datawave.query.analysis.QueryAnalyzer.Syntax;

/** An immutable labeled raw query. Labels express desired grouping, not persistent classification names. */
public final class QueryTuningSample {
    private final String id;
    private final String query;
    private final Syntax syntax;
    private final String bucket;

    public QueryTuningSample(String query, Syntax syntax, String bucket) {
        this(null, query, syntax, bucket);
    }

    public QueryTuningSample(String id, String query, Syntax syntax, String bucket) {
        if (id != null) {
            requireNonblank(id, "id");
        }
        this.id = id;
        this.query = requireNonblank(query, "query");
        this.syntax = Objects.requireNonNull(syntax, "syntax");
        this.bucket = requireNonblank(bucket, "bucket");
    }

    private static String requireNonblank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must be a nonblank string");
        }
        return value;
    }

    /** Optional caller-provided identifier; null when omitted. */
    public String getId() {
        return id;
    }

    public String getQuery() {
        return query;
    }

    public Syntax getSyntax() {
        return syntax;
    }

    public String getBucket() {
        return bucket;
    }

    public QueryInput toQueryInput() {
        return new QueryInput(query, syntax);
    }
}
