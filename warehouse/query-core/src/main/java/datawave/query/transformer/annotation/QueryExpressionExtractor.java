package datawave.query.transformer.annotation;

import java.io.Serializable;

/** Extracts structured, positive search expressions from a query syntax. */
@FunctionalInterface
public interface QueryExpressionExtractor extends Serializable {
    SearchExpressions extract(String query);
}
