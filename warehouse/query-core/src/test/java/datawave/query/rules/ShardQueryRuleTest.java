package datawave.query.rules;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ParseException;
import org.apache.lucene.queryparser.flexible.core.QueryNodeParseException;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.junit.jupiter.api.AfterEach;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.microservice.query.Query;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.language.parser.lucene.AccumuloSyntaxParser;
import datawave.query.util.MetadataHelper;
import datawave.query.util.TypeMetadata;
import datawave.test.QueryRuleResultAssert;

public abstract class ShardQueryRuleTest {

    protected static final String RULE_NAME = "RuleUnderTest";
    protected static final AccumuloSyntaxParser luceneParser = new AccumuloSyntaxParser();

    protected String ruleName;
    protected String query;
    protected MetadataHelper metadataHelper;
    protected TypeMetadata typeMetadata;
    protected Query querySettings;
    protected GenericQueryConfiguration queryConfiguration;

    protected String expectedRuleName;
    protected Exception expectedException;
    protected final List<String> expectedMessages = new ArrayList<>();

    @AfterEach
    void tearDown() {
        this.ruleName = null;
        this.query = null;
        this.metadataHelper = null;
        this.typeMetadata = null;
        this.querySettings = null;
        this.queryConfiguration = null;
        this.expectedRuleName = null;
        this.expectedException = null;
        this.expectedMessages.clear();
    }

    protected void givenRuleName(String ruleName) {
        this.ruleName = ruleName;
    }

    protected void givenQuery(String query) {
        this.query = query;
    }

    protected void givenMetadataHelper(MetadataHelper metadataHelper) {
        this.metadataHelper = metadataHelper;
    }

    protected void givenTypeMetadata(TypeMetadata typeMetadata) {
        this.typeMetadata = typeMetadata;
    }

    protected void givenQuerySettings(Query querySettings) {
        this.querySettings = querySettings;
    }

    protected void givenQueryConfiguration(GenericQueryConfiguration queryConfiguration) {
        this.queryConfiguration = queryConfiguration;
    }

    protected void expectRuleName(String ruleName) {
        this.expectedRuleName = ruleName;
    }

    protected void expectException(Exception exception) {
        this.expectedException = exception;
    }

    protected void expectMessage(String message) {
        this.expectedMessages.add(message);
    }

    protected abstract Object parseQuery() throws Exception;

    protected ASTJexlScript parseQueryToJexl() throws ParseException {
        return JexlASTHelper.parseJexlQuery(query);
    }

    protected QueryNode parseQueryToLucene() throws QueryNodeParseException {
        return luceneParser.parse(query, "");
    }

    protected abstract ShardQueryRule getNewRule();

    protected ShardQueryValidationConfiguration getValidationConfiguration() throws Exception {
        ShardQueryValidationConfiguration configuration = new ShardQueryValidationConfiguration();
        configuration.setParsedQuery(parseQuery());
        configuration.setMetadataHelper(metadataHelper);
        configuration.setTypeMetadata(typeMetadata);
        configuration.setQuerySettings(querySettings);
        configuration.setQueryConfiguration(queryConfiguration);
        return configuration;
    }

    protected void assertResult() throws Exception {
        ShardQueryRule rule = getNewRule();
        rule.setName(ruleName);

        ShardQueryValidationConfiguration validationConfiguration = getValidationConfiguration();
        QueryRuleResult result = rule.validate(validationConfiguration);
        // @formatter:off
        QueryRuleResultAssert.assertThat(result)
                        .hasRuleName(expectedRuleName)
                        .hasException(expectedException)
                        .hasExactMessagesInAnyOrder(expectedMessages);
        // @formatter:on
    }
}
