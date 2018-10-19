package datawave.query.language.parser.jexl;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import datawave.query.language.builder.jexl.JexlTreeBuilder;
import datawave.query.language.functions.jexl.JexlQueryFunction;
import datawave.query.language.parser.ParseException;
import datawave.query.language.parser.QueryParser;
import datawave.query.language.parser.lucene.AccumuloSyntaxParser;
import datawave.query.language.parser.lucene.QueryConfigHandler;
import datawave.query.language.processor.lucene.CustomQueryNodeProcessorPipeline;
import datawave.query.language.tree.QueryNode;
import datawave.query.language.tree.ServerHeadNode;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.flexible.core.builders.QueryBuilder;
import org.apache.lucene.queryparser.flexible.core.config.ConfigurationKey;
import org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessor;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler.ConfigurationKeys;
import org.apache.lucene.util.Version;

public class LuceneToJexlQueryParser implements QueryParser {
    private static final String[] DEFAULT_TOKENIZED_FIELDS = {"TOKFIELD"};
    private static final String[] DEFAULT_SKIP_TOKENIZE_UNFIELDED_FIELDS = {"NOTOKEN"};
    
    private Set<String> tokenizedFields = new HashSet<>();
    private boolean tokenizeUnfieldedQueries = false;
    private boolean allowLeadingWildCard = true;
    private boolean tokensAsPhrase = false;
    private Set<String> skipTokenizeUnfieldedFields = new HashSet<>();
    
    private boolean positionIncrementsEnabled = true;
    private Set<String> allowedFields = null;
    private Boolean allowAnyFieldQueries = true;
    private List<JexlQueryFunction> allowedFunctions = JexlTreeBuilder.DEFAULT_ALLOWED_FUNCTION_LIST;
    private Analyzer analyzer = new StandardAnalyzer();
    
    public LuceneToJexlQueryParser() {
        Collections.addAll(tokenizedFields, DEFAULT_TOKENIZED_FIELDS);
        Collections.addAll(skipTokenizeUnfieldedFields, DEFAULT_SKIP_TOKENIZE_UNFIELDED_FIELDS);
    }
    
    public static final ConfigurationKey<Set<String>> TOKENIZED_FIELDS = ConfigurationKey.newInstance();
    public static final ConfigurationKey<Boolean> TOKENIZE_UNFIELDED_QUERIES = ConfigurationKey.newInstance();
    public static final ConfigurationKey<Set<String>> SKIP_TOKENIZE_UNFIELDED_FIELDS = ConfigurationKey.newInstance();
    public static final ConfigurationKey<Set<String>> ALLOWED_FIELDS = ConfigurationKey.newInstance();
    public static final ConfigurationKey<Boolean> ALLOW_ANY_FIELD_QUERIES = ConfigurationKey.newInstance();
    public static final ConfigurationKey<Boolean> TOKENS_AS_PHRASE = ConfigurationKey.newInstance();
    
    @Override
    public QueryNode parse(String query) throws ParseException {
        JexlNode parsedQuery = convertToJexlNode(query);
        QueryNode node = new ServerHeadNode();
        node.setOriginalQuery(parsedQuery.toString());
        return node;
    }
    
    public JexlNode convertToJexlNode(String query) throws ParseException {
        query = query.replaceAll("\\u0093", "\""); // replace open smart quote 147
        query = query.replaceAll("\\u0094", "\""); // replace close smart quote 148
        
        query = query.replaceAll("\\u201c", "\""); // replace open left double quote
        query = query.replaceAll("\\u201d", "\""); // replace close right double quote
        
        JexlNode parsedQuery = null;
        
        try {
            Locale.setDefault(Locale.US);
            AccumuloSyntaxParser syntaxParser = new AccumuloSyntaxParser();
            syntaxParser.enable_tracing();
            
            QueryConfigHandler queryConfigHandler = new QueryConfigHandler();
            
            queryConfigHandler.set(ConfigurationKeys.ANALYZER, analyzer);
            
            queryConfigHandler.set(ConfigurationKeys.ENABLE_POSITION_INCREMENTS, positionIncrementsEnabled);
            queryConfigHandler.set(TOKENIZED_FIELDS, tokenizedFields);
            queryConfigHandler.set(TOKENIZE_UNFIELDED_QUERIES, tokenizeUnfieldedQueries);
            queryConfigHandler.set(SKIP_TOKENIZE_UNFIELDED_FIELDS, skipTokenizeUnfieldedFields);
            queryConfigHandler.set(ALLOWED_FIELDS, allowedFields);
            queryConfigHandler.set(ALLOW_ANY_FIELD_QUERIES, allowAnyFieldQueries);
            queryConfigHandler.set(TOKENS_AS_PHRASE, tokensAsPhrase);
            
            queryConfigHandler.set(ConfigurationKeys.ALLOW_LEADING_WILDCARD, allowLeadingWildCard);
            
            QueryNodeProcessor processor = new CustomQueryNodeProcessorPipeline(queryConfigHandler);
            QueryBuilder builder = new JexlTreeBuilder(allowedFunctions);
            
            org.apache.lucene.queryparser.flexible.core.nodes.QueryNode queryTree = syntaxParser.parse(query, "");
            queryTree = processor.process(queryTree);
            parsedQuery = (JexlNode) builder.build(queryTree);
        } catch (Exception e) {
            throw new ParseException(e);
        }
        return parsedQuery;
    }
    
    public boolean isTokenizeUnfieldedQueries() {
        return tokenizeUnfieldedQueries;
    }
    
    public void setTokenizeUnfieldedQueries(boolean tokenizeUnfieldedQueries) {
        this.tokenizeUnfieldedQueries = tokenizeUnfieldedQueries;
    }
    
    public boolean isPositionIncrementsEnabled() {
        return positionIncrementsEnabled;
    }
    
    public void setPositionIncrementsEnabled(boolean positionIncrementsEnabled) {
        this.positionIncrementsEnabled = positionIncrementsEnabled;
    }
    
    public Set<String> getTokenizedFields() {
        return tokenizedFields;
    }
    
    public void setTokenizedFields(Set<String> tokenizedFields) {
        this.tokenizedFields = tokenizedFields;
    }
    
    public Set<String> getSkipTokenizeUnfieldedFields() {
        return skipTokenizeUnfieldedFields;
    }
    
    public void setSkipTokenizeUnfieldedFields(Set<String> skipTokenizeUnfieldedFields) {
        this.skipTokenizeUnfieldedFields = skipTokenizeUnfieldedFields;
    }
    
    public boolean isAllowLeadingWildCard() {
        return allowLeadingWildCard;
    }
    
    public void setAllowLeadingWildCard(boolean allowLeadingWildCard) {
        this.allowLeadingWildCard = allowLeadingWildCard;
    }
    
    public Set<String> getAllowedFields() {
        return allowedFields;
    }
    
    public void setAllowedFields(Set<String> allowedFields) {
        this.allowedFields = allowedFields;
    }
    
    public Boolean getAllowAnyField() {
        return allowAnyFieldQueries;
    }
    
    public void setAllowAnyField(Boolean allowAnyField) {
        this.allowAnyFieldQueries = allowAnyField;
    }
    
    public boolean isTokensAsPhrase() {
        return tokensAsPhrase;
    }
    
    public void setTokensAsPhrase(boolean tokensAsPhrase) {
        this.tokensAsPhrase = tokensAsPhrase;
    }
    
    public Analyzer getAnalyzer() {
        return analyzer;
    }
    
    public void setAnalyzer(Analyzer analyzer) {
        this.analyzer = analyzer;
    }
    
    public List<JexlQueryFunction> getAllowedFunctions() {
        return allowedFunctions;
    }
    
    public void setAllowedFunctions(List<JexlQueryFunction> allowedFunctions) {
        this.allowedFunctions = allowedFunctions;
    }
}
