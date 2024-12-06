package datawave.query.language.parser;

import datawave.query.language.tree.QueryNode;

public interface QueryParser {
    QueryNode parse(String query) throws ParseException;
}
