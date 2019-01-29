package datawave.query.jexl.visitors;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.util.MetadataHelperFactory;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.junit.Assert;
import org.junit.Test;

public class ExpandMultiNormalizedTermsTest {
    
    @Test
    public void testQueryThatShouldMakeNoRanges() throws Exception {
        
        String query = "FOO == 125 " + " AND (BAR).getValuesForGroups(grouping:getGroupsForMatchesInGroup(FOO,'125')) >= 35"
                        + " and (BAR).getValuesForGroups(grouping:getGroupsForMatchesInGroup(FOO,'125')) <= 36"
                        + " and (BAZ).getValuesForGroups(grouping:getGroupsForMatchesInGroup(FOO,'125')) >= 25"
                        + " and (BAZ).getValuesForGroups(grouping:getGroupsForMatchesInGroup(FOO,'125')) <= 26" + " and WHATEVER == 'la'";
        
        ASTJexlScript queryTree = JexlASTHelper.parseJexlQuery(query);
        
        ASTJexlScript smashed = TreeFlatteningRebuildingVisitor.flatten(queryTree);
        
        ASTJexlScript script = (ASTJexlScript) smashed.jjtAccept(
                        new ExpandMultiNormalizedTerms(new ShardQueryConfiguration(), new MetadataHelperFactory().createMetadataHelper()), null);
        
        String originalRoundTrip = JexlStringBuildingVisitor.buildQuery(queryTree);
        String smashedRoundTrip = JexlStringBuildingVisitor.buildQuery(smashed);
        String visitedRountTrip = JexlStringBuildingVisitor.buildQuery(script);
        
        Assert.assertEquals(originalRoundTrip, smashedRoundTrip);
        Assert.assertEquals(smashedRoundTrip, visitedRountTrip);
        
    }
}
