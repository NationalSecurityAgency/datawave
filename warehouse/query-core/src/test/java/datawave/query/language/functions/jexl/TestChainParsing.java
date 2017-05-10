package datawave.query.language.functions.jexl;

import java.util.ArrayList;
import java.util.List;

import datawave.query.language.parser.jexl.LuceneToJexlQueryParser;
import datawave.query.language.tree.QueryNode;

import org.apache.lucene.queryparser.flexible.core.nodes.FunctionQueryNode;
import org.apache.lucene.queryparser.flexible.core.util.UnescapedCharSequence;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestChainParsing {
    
    LuceneToJexlQueryParser parser = null;
    
    @Before
    public void setup() {
        parser = new LuceneToJexlQueryParser();
        List<JexlQueryFunction> allowedFunctions = new ArrayList<>();
        allowedFunctions.add(new ChainStart());
        allowedFunctions.add(new Chain());
        parser.setAllowedFunctions(allowedFunctions);
    }
    
    // chainstart(EventQuery, 'field1:term1 AND field2:term3', 20140101, 20140731, 'params')
    // chain('a=field4, b=[field6|field7]', EventQuery, 'field4:${field4} AND fields:${field5}', 20140101, 20140731, params)
    
    @Test
    public void testChainParsingWithParams() {
        try {
            QueryNode node = parser
                            .parse("#CHAINSTART('EventQuery', 'FIELD1:term1 AND FIELD2:term2', '20140801', '20140830', 'query.syntax:LUCENE') #CHAIN('a=RESULT_FIELD', 'EventQuery', 'FIELD3:${a} AND FIELD4:term4', '20140801', '20140830', 'query.syntax:LUCENE')");
            String originalQuery = node.getOriginalQuery();
            String querySplit[] = originalQuery.split("&&");
            
            UnescapedCharSequence s1 = new UnescapedCharSequence(querySplit[0].trim());
            FunctionQueryNode functionQueryNode1 = new FunctionQueryNode(s1, 0, s1.length());
            Assert.assertEquals("CHAINSTART", functionQueryNode1.getFunction());
            Assert.assertEquals(5, functionQueryNode1.getParameterList().size());
            
            UnescapedCharSequence s2 = new UnescapedCharSequence(querySplit[1].trim());
            FunctionQueryNode functionQueryNode2 = new FunctionQueryNode(s2, 0, s2.length());
            Assert.assertEquals("CHAIN", functionQueryNode2.getFunction());
            Assert.assertEquals(6, functionQueryNode2.getParameterList().size());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }
    
    @Test
    public void testChainParsingWithNoParams() {
        try {
            QueryNode node = parser
                            .parse("#CHAINSTART('EventQuery', 'FIELD1:term1 AND FIELD2:term2', '20140801', '20140830') #CHAIN('a=RESULT_FIELD', 'EventQuery', 'FIELD3:${a} AND FIELD4:term4', '20140801', '20140830')");
            String originalQuery = node.getOriginalQuery();
            String querySplit[] = originalQuery.split("&&");
            
            UnescapedCharSequence s1 = new UnescapedCharSequence(querySplit[0].trim());
            FunctionQueryNode functionQueryNode1 = new FunctionQueryNode(s1, 0, s1.length());
            Assert.assertEquals("CHAINSTART", functionQueryNode1.getFunction());
            Assert.assertEquals(4, functionQueryNode1.getParameterList().size());
            
            UnescapedCharSequence s2 = new UnescapedCharSequence(querySplit[1].trim());
            FunctionQueryNode functionQueryNode2 = new FunctionQueryNode(s2, 0, s2.length());
            Assert.assertEquals("CHAIN", functionQueryNode2.getFunction());
            Assert.assertEquals(5, functionQueryNode2.getParameterList().size());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }
}
