package datawave.query.jexl.visitors;

import static java.util.Collections.emptyList;

import static datawave.query.jexl.JexlASTHelper.jexlFeatures;
import static org.junit.Assert.assertThrows;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.Parser;
import org.apache.commons.jexl3.parser.StringProvider;
import org.junit.Before;
import org.junit.Test;

import datawave.edge.model.DefaultEdgeModelFieldsFactory;
import datawave.query.tables.edge.EdgeQueryLogic;

public class EdgeTableRangeBuildingVisitorTest {

    private int termLimit = 3;
    private Parser parser;

    private EdgeTableRangeBuildingVisitor visitor;

    @Before
    public void setup() {
        parser = new Parser(new StringProvider(";"));

        visitor = new EdgeTableRangeBuildingVisitor(false, emptyList(), termLimit, emptyList(), new DefaultEdgeModelFieldsFactory().createFields());
    }

    @Test
    public void shouldEnforceTermLimit() {
        ASTJexlScript parsedQuery = parseQuery("TYPE == 'like it' OR TYPE == 'love it' OR TYPE == 'gotta have it' OR TYPE == 'hand it over or else'");

        assertThrows(IllegalArgumentException.class, () -> parsedQuery.jjtAccept(visitor, null));
    }

    private ASTJexlScript parseQuery(String query) {
        return parser.parse(null, jexlFeatures(), EdgeQueryLogic.fixQueryString(query), null);
    }
}
