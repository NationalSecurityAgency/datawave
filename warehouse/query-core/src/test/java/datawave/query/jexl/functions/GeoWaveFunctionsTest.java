package datawave.query.jexl.functions;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import datawave.query.language.parser.jexl.LuceneToJexlQueryParser;
import datawave.query.language.tree.QueryNode;

public class GeoWaveFunctionsTest {

    @Test
    public void testLuceneToJexlConversion() throws Exception {
        LuceneToJexlQueryParser parser = new LuceneToJexlQueryParser();

        QueryNode node = null;

        node = parser.parse("#CONTAINS(FIELD, 'POINT(10 20)')");
        assertEquals("geowave:contains(FIELD, 'POINT(10 20)')", node.getOriginalQuery());

        node = parser.parse("#COVERS(FIELD, 'POINT(10 20)')");
        assertEquals("geowave:covers(FIELD, 'POINT(10 20)')", node.getOriginalQuery());

        node = parser.parse("#COVERED_BY(FIELD, 'POINT(10 20)')");
        assertEquals("geowave:covered_by(FIELD, 'POINT(10 20)')", node.getOriginalQuery());

        node = parser.parse("#CROSSES(FIELD, 'POINT(10 20)')");
        assertEquals("geowave:crosses(FIELD, 'POINT(10 20)')", node.getOriginalQuery());

        node = parser.parse("#INTERSECTS(FIELD, 'POINT(10 20)')");
        assertEquals("geowave:intersects(FIELD, 'POINT(10 20)')", node.getOriginalQuery());

        node = parser.parse("#OVERLAPS(FIELD, 'POINT(10 20)')");
        assertEquals("geowave:overlaps(FIELD, 'POINT(10 20)')", node.getOriginalQuery());

        node = parser.parse("#WITHIN(FIELD, 'POINT(10 20)')");
        assertEquals("geowave:within(FIELD, 'POINT(10 20)')", node.getOriginalQuery());
    }
}
