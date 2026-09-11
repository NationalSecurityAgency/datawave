package datawave.query.transformer.annotation;

import java.io.Serializable;

/** Parses the explicit all-hits keyword override into structured expressions. */
@FunctionalInterface
public interface KeywordParser extends Serializable {
    SearchExpressions parse(String keywords);
}
