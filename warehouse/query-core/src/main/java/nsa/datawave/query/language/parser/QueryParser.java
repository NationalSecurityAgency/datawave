package nsa.datawave.query.language.parser;

import nsa.datawave.query.language.tree.QueryNode;

public interface QueryParser {
    public QueryNode parse(String query) throws ParseException;
}
