package datawave.query.language.parser;

import org.apache.lucene.queryparser.flexible.core.parser.SyntaxParser;

import datawave.query.language.tree.QueryNode;

public interface QueryParser {
    QueryNode parse(String query) throws ParseException;
}
