package datawave.query.language.parser;

import datawave.query.language.tree.QueryNode;

public interface QueryParser {
    public QueryNode parse(String query) throws ParseException;
}
