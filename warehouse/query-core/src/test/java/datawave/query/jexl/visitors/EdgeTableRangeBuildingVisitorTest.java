package datawave.query.jexl.visitors;

import static java.util.Collections.emptyList;

import static org.junit.Assert.assertThrows;

import java.io.StringReader;

import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.commons.jexl2.parser.Parser;
import org.junit.Before;
import org.junit.Test;

import datawave.query.tables.edge.EdgeQueryLogic;

public class EdgeTableRangeBuildingVisitorTest {

    private int termLimit = 3;
    private Parser parser;

    private EdgeTableRangeBuildingVisitor visitor;

    @Before
    public void setup() {
        parser = new Parser(new StringReader(";"));

        visitor = new EdgeTableRangeBuildingVisitor(false, emptyList(), termLimit, emptyList());
    }

    @Test
    public void shouldEnforceTermLimit() throws ParseException {
        ASTJexlScript parsedQuery = parseQuery("TYPE == 'like it' OR TYPE == 'love it' OR TYPE == 'gotta have it' OR TYPE == 'hand it over or else'");

        assertThrows(IllegalArgumentException.class, () -> parsedQuery.jjtAccept(visitor, null));
    }

    private ASTJexlScript parseQuery(String query) throws ParseException {
        return parser.parse(new StringReader(EdgeQueryLogic.fixQueryString(query)), null);
    }
}
