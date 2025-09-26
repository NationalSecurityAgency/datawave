package datawave.query.language.parser.lucene;

import org.apache.lucene.queryparser.flexible.core.QueryNodeParseException;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;

import datawave.query.language.parser.QueryParser;

public interface LuceneSyntaxQueryParser extends QueryParser {

    QueryNode parseToLuceneQueryNode(String query) throws QueryNodeParseException;
}
