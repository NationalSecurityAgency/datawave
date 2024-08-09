package datawave.core.query.language.parser.lucene;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.log4j.Logger;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.builders.QueryBuilder;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessor;

import datawave.core.query.language.builder.lucene.AccumuloQueryTreeBuilder;
import datawave.core.query.language.functions.lucene.LuceneQueryFunction;
import datawave.core.query.language.parser.ParseException;
import datawave.core.query.language.parser.QueryParser;
import datawave.core.query.language.processor.lucene.CustomQueryNodeProcessorPipeline;
import datawave.core.query.language.tree.FunctionNode;
import datawave.core.query.language.tree.HardAndNode;
import datawave.core.query.language.tree.NotNode;
import datawave.core.query.language.tree.SelectorNode;
import datawave.core.query.language.tree.SoftAndNode;
import datawave.core.query.search.FieldedTerm;
import datawave.core.query.search.RangeFieldedTerm;
import datawave.core.query.search.Term;

public class LuceneQueryParser implements QueryParser {
    private static Logger log = Logger.getLogger(LuceneQueryParser.class.getName());
    private Map<String,String> filters = new HashMap<>();
    private List<LuceneQueryFunction> allowedFunctions = null;

    @Override
    public datawave.core.query.language.tree.QueryNode parse(String query) throws ParseException {
        query = query.replaceAll("\\u0093", "\""); // replace open smart quote 147
        query = query.replaceAll("\\u0094", "\""); // replace close smart quote 148

        datawave.core.query.language.tree.QueryNode parsedQuery = null;

        try {
            Locale.setDefault(Locale.US);
            AccumuloSyntaxParser syntaxParser = new AccumuloSyntaxParser();
            syntaxParser.enable_tracing();

            org.apache.lucene.queryparser.flexible.core.config.QueryConfigHandler queryConfigHandler = new QueryConfigHandler();
            QueryNodeProcessor processor = new CustomQueryNodeProcessorPipeline(queryConfigHandler);
            QueryBuilder builder = null;
            if (allowedFunctions == null) {
                builder = new AccumuloQueryTreeBuilder();
            } else {
                builder = new AccumuloQueryTreeBuilder(allowedFunctions);
            }

            QueryNode queryTree = syntaxParser.parse(query, "");
            queryTree = processor.process(queryTree);
            parsedQuery = (datawave.core.query.language.tree.QueryNode) builder.build(queryTree);

            Set<FieldedTerm> positiveFilters = new TreeSet<>();

            if (log.isTraceEnabled()) {
                log.trace("Query before filters extracted: " + parsedQuery.getContents());
            }
            extractFilters(parsedQuery, positiveFilters);

            parsedQuery.setPositiveFilters(positiveFilters);
            if (log.isTraceEnabled()) {
                log.trace("Query after filters extracted: " + parsedQuery.getContents());
            }
        } catch (QueryNodeException | RuntimeException e) {
            throw new ParseException(e);
        }

        return parsedQuery;
    }

    public Map<String,String> getFilters() {
        return filters;
    }

    public void setFilters(Map<String,String> filters) {
        this.filters = filters;
    }

    private void extractFilters(datawave.core.query.language.tree.QueryNode node, Set<FieldedTerm> filters) {
        if (node instanceof FunctionNode) {
            throw new IllegalArgumentException("Insufficient terms to evaluate");
        } else if (!(node instanceof SelectorNode)) {
            Set<FieldedTerm> alwaysFilterTerms = new HashSet<>();
            List<datawave.core.query.language.tree.QueryNode> childrenList = node.getChildren();
            extractAlwaysFilterNodes(alwaysFilterTerms, childrenList);
            filters.addAll(alwaysFilterTerms);
            node.setChildren(childrenList);
        }

        if (node instanceof SoftAndNode || node instanceof HardAndNode) {
            List<datawave.core.query.language.tree.QueryNode> evaluateNodes = new ArrayList<>();
            evaluateNodes.addAll(node.getChildren());
            Set<FieldedTerm> possibleFilters = extractFilters(evaluateNodes);

            if (!evaluateNodes.isEmpty()) {
                filters.addAll(possibleFilters);

                for (datawave.core.query.language.tree.QueryNode n : evaluateNodes) {
                    extractFilters(n, filters);
                }
                node.setChildren(evaluateNodes);
            }

        } else if (node instanceof NotNode) {
            List<datawave.core.query.language.tree.QueryNode> children = node.getChildren();

            // If the first child of a NOT node might have filterable fields in it
            datawave.core.query.language.tree.QueryNode positiveNode = children.get(0);
            extractFilters(positiveNode, filters);
        }
    }

    private Set<FieldedTerm> extractAlwaysFilterNodes(Set<FieldedTerm> possibleFilters, List<datawave.core.query.language.tree.QueryNode> nodes) {
        ListIterator<datawave.core.query.language.tree.QueryNode> itr = nodes.listIterator(nodes.size());

        while (itr.hasPrevious() && !nodes.isEmpty()) {
            datawave.core.query.language.tree.QueryNode n = itr.previous();
            if (n instanceof FunctionNode) {
                FunctionNode fNode = (FunctionNode) n;
                possibleFilters.add((FieldedTerm) fNode.getQuery());
                itr.remove();
            } else {
                List<datawave.core.query.language.tree.QueryNode> children = n.getChildren();
                if (!children.isEmpty()) {
                    extractAlwaysFilterNodes(possibleFilters, children);
                    n.setChildren(children);
                }
            }
        }
        if (nodes.isEmpty()) {
            throw new IllegalArgumentException("Insufficient terms to evaluate");
        }
        return possibleFilters;
    }

    private Set<FieldedTerm> extractFilters(List<datawave.core.query.language.tree.QueryNode> nodes) {
        Set<FieldedTerm> possibleFilters = new TreeSet<>();

        ListIterator<datawave.core.query.language.tree.QueryNode> itr = nodes.listIterator(nodes.size());

        // Don't remove the last remaining node, even if it is filter-able
        // traverse in reverse order since users have been trained to place high cardinality
        // fields at the end of the query -- we want to filter those first if possible
        while (itr.hasPrevious() && nodes.size() > 1) {
            datawave.core.query.language.tree.QueryNode n = itr.previous();
            boolean filterFound = false;
            if (n instanceof SelectorNode) {
                Term term = ((SelectorNode) n).getQuery();
                if (term != null && term instanceof FieldedTerm) {
                    FieldedTerm queryTerm = (FieldedTerm) term;
                    for (Map.Entry<String,String> filterCandidate : filters.entrySet()) {
                        if (filterCandidate.getKey().equals(queryTerm.getField())) {
                            // compare selectors
                            if (queryTerm instanceof RangeFieldedTerm) {
                                filterFound = true;
                                possibleFilters.add(queryTerm);
                                break;
                            } else if (queryTerm.getSelector().matches(filterCandidate.getValue())) {
                                filterFound = true;
                                possibleFilters.add(queryTerm);
                                break;
                            }
                        }
                    }
                }
            }
            if (filterFound) {
                itr.remove();
            }
        }
        return possibleFilters;
    }

    public List<LuceneQueryFunction> getAllowedFunctions() {
        return allowedFunctions;
    }

    public void setAllowedFunctions(List<LuceneQueryFunction> allowedFunctions) {
        this.allowedFunctions = allowedFunctions;
    }
}
