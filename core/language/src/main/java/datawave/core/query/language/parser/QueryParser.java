package datawave.core.query.language.parser;

import datawave.core.query.language.tree.QueryNode;

public interface QueryParser {
    QueryNode parse(String query) throws ParseException;
}
