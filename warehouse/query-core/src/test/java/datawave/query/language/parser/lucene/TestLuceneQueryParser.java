package datawave.query.language.parser.lucene;

import java.util.HashMap;
import java.util.Map;

import datawave.query.language.parser.ParseException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestLuceneQueryParser {
    
    private LuceneQueryParser queryParser;
    
    @Before
    public void setUp() {
        queryParser = new LuceneQueryParser();
    }
    
    @Test
    public void testFunctionArgumentEscaping() throws ParseException {
        Assert.assertEquals("[AND,Field:Selector][posFilter: filter(true, AND, FIELD1, rege(x), FIELD2, rege(x))]",
                        queryParser.parse("Field:Selector AND #INCLUDE(AND, FIELD1, 'rege(x)', FIELD2, 'rege(x)')").getContents());
        Assert.assertEquals("[AND,Field:Selector][posFilter: filter(true, AND, FIELD1, rege(x), FIELD2, reg,e(x))]",
                        queryParser.parse("Field:Selector AND #INCLUDE(AND, FIELD1, 'rege(x)', FIELD2, 'reg,e(x)')").getContents());
        Assert.assertEquals("[AND,Field:Selector][posFilter: filter(true, AND, FIELD1, rege(x), FIELD2, rege(x))]",
                        queryParser.parse("Field:Selector AND #INCLUDE(AND, FIELD1, 'rege(x)', FIELD2, \"rege(x)\")").getContents());
        Assert.assertEquals("[AND,Field:Selector][posFilter: filter(true, AND, FIELD1, rege(x), FIELD2, reg,e(x))]",
                        queryParser.parse("Field:Selector AND #INCLUDE(AND, FIELD1, \"rege(x)\", FIELD2, 'reg,e(x)')").getContents());
        Assert.assertEquals("[AND,F:S][posFilter: filter(true, AND, FIELD, regex)]", queryParser.parse("F:S AND #INCLUDE(FIELD, regex)").getContents());
        Assert.assertEquals("[AND,Field:Selector][posFilter: filter(true, AND, FIELD1, rege(x), FIELD2, rege(x))]",
                        queryParser.parse("Field:Selector AND #INCLUDE(AND, FIELD1, 'rege(x)' , FIELD2 , \"rege(x)\" )").getContents());
        Assert.assertEquals("[AND,Field:Selector][posFilter: filter(true, AND, FIELD1, rege(x), FIELD2,  rege(x))]",
                        queryParser.parse("Field:Selector AND #INCLUDE(AND, FIELD1, 'rege(x)', FIELD2 ,\" rege(x)\" )").getContents());
    }
    
    @Test
    public void testAngledBrackets() throws ParseException {
        // if angled brackets need to be escaped, this query will fail to parse
        Assert.assertEquals("000.000.000<realm>", queryParser.parse("000.000.000<realm>").toString());
    }
    
    @Test
    public void testEscapeBackslash() throws ParseException {
        // user types ab\\cd\\e to search for ab\cd\e
        Assert.assertEquals("ab\\\\cd\\\\e", queryParser.parse("ab\\\\cd\\\\e").getContents());
        
        // user types ab\\cde to search for ab\cde
        Assert.assertEquals("ab\\\\cde", queryParser.parse("ab\\\\cde").getContents());
        
        // user types ab\cde to search for abcde, i.e. the c is 'escaped and the \ removed'
        Assert.assertEquals("abcde", queryParser.parse("ab\\cde").getContents());
    }
    
    @Test
    public void testPhraseCharacters() throws ParseException {
        Assert.assertEquals("esca\"ped", queryParser.parse("\"esca\\\"ped\"").getContents());
    }
    
    @Test
    public void testEscapeWildcards() throws ParseException {
        // the saved selector doesn't have the \* \? or \\ removed so we can tell the
        // difference between an escaped wildcard and an unescaped wildcard
        
        Assert.assertEquals("a\\*?\\?b", queryParser.parse("a\\*?\\?b").toString());
        Assert.assertEquals("abc*de\\**fghij*klm?nopq\\??rs", queryParser.parse("abc*de\\**fghij*klm?nopq\\??rs").toString());
    }
    
    @Test(expected = ParseException.class)
    public void testParseExceptionBadNot() throws ParseException {
        queryParser.parse("a not");
    }
    
    @Test(expected = ParseException.class)
    public void testParseExceptionTooManyLParens() throws ParseException {
        queryParser.parse("((almostgood)");
    }
    
    @Test(expected = ParseException.class)
    public void testParseExceptionTooManyRParens() throws ParseException {
        queryParser.parse("data value)");
    }
    
    @Test(expected = ParseException.class)
    public void testParseExceptionNotEnoughParens() throws ParseException {
        queryParser.parse("(this AND isbadquery");
    }
    
    @Test
    public void testParse() throws ParseException {
        Assert.assertEquals("a*", queryParser.parse("a*").getContents());
        Assert.assertEquals("[ADJ4,cat,and,dog,within,5]", queryParser.parse("\"cat and dog within 5\"").getContents());
        Assert.assertEquals("[ADJ2,Loc:a,Loc:b,Loc:c]", queryParser.parse("Loc:\"a b c\"").getContents());
        Assert.assertEquals("[NOT,dsafd,jjksl]", queryParser.parse("dsafd not jjksl").getContents());
        Assert.assertEquals("[AND,111,222]", queryParser.parse("111 222").getContents());
        Assert.assertEquals("ID:345", queryParser.parse("(ID:345)").getContents());
        Assert.assertEquals("[AND,1234,ae$%^&]", queryParser.parse("1234 AND ae$%^&").getContents());
        Assert.assertEquals("[OR,abc,cdf]", queryParser.parse("abc OR cdf").getContents());
        Assert.assertEquals("[NOT,a,b,c]", queryParser.parse("a not b not c").getContents());
        Assert.assertEquals("[AND,[AND,a,d],[OR,b,c]]", queryParser.parse("(a AND d) AND (b OR c)").getContents());
        Assert.assertEquals("[OR,a,[AND,b,[NOT,c,[AND,[AND,d,e],f]]]]", queryParser.parse("a OR (b AND (c NOT ((d e) f)))").getContents());
        Assert.assertEquals("[AND,and,or]", queryParser.parse("\"and\" AND \"or\"").getContents());
        Assert.assertEquals("[AND,[ADJ1,exact,phrase],word]", queryParser.parse("\"exact phrase\" AND word").getContents());
        Assert.assertEquals("[NOT,[AND,a,all,of,these,any,word],[AND,none,can,be,here,[ADJ1,exact,phrase]]]",
                        queryParser.parse("(a all of these any word) NOT (none can be here \"exact phrase\")").getContents());
        
        Assert.assertEquals("[AND,alpha,[AND,gamma,delta]]", queryParser.parse("alpha AND gamma delta").getContents());
        Assert.assertEquals("[AND,[AND,alpha,beta],[AND,gamma,delta]]", queryParser.parse("alpha beta AND gamma delta").getContents());
        Assert.assertEquals("[OR,[AND,alpha,beta],[AND,gamma,delta]]", queryParser.parse("alpha beta OR gamma delta").getContents());
        Assert.assertEquals("[AND,[AND,alpha,beta],[AND,gamma,delta]]", queryParser.parse("(alpha beta) AND (gamma delta)").getContents());
        Assert.assertEquals("[NOT,[AND,alpha,beta],[AND,gamma,delta]]", queryParser.parse("(alpha beta) NOT (gamma delta)").getContents());
        Assert.assertEquals("[NOT,[AND,alpha,beta],[AND,gamma,delta]]", queryParser.parse("(alpha beta) NOT gamma delta").getContents());
        
        // test new order of operation NOT, AND, and
        Assert.assertEquals("[NOT,[AND,alpha,beta],[AND,gamma,delta]]", queryParser.parse("alpha beta NOT gamma delta").getContents());
        
        Assert.assertEquals("[AND,alpha,[NOT,beta,gamma],delta]", queryParser.parse("alpha (beta NOT gamma) delta").getContents());
        
        Assert.assertEquals("[NOT,[AND,alpha,beta],gamma,delta]", queryParser.parse("alpha beta NOT gamma NOT delta").getContents());
        
        Assert.assertEquals("[NOT,[AND,alpha,beta],[OR,gamma,delta]]", queryParser.parse("alpha beta NOT (gamma OR delta)").getContents());
        
        Assert.assertEquals("[NOT,[OR,alpha,beta],[AND,gamma,delta]]", queryParser.parse("alpha or beta NOT gamma delta").getContents());
        
        // make sure that the parser can ignore leading and trailing whitespace
        Assert.assertEquals("selector1", queryParser.parse("\" selector1 \"").getContents());
        
        Assert.assertEquals("[ADJ1,selector1,selector2]", queryParser.parse("\" selector1  selector2  \"").getContents());
        
        Assert.assertEquals("[ADJ2,selector1,selector2,selector3]", queryParser.parse("\" selector1  selector2  selector3 \"").getContents());
        
        Assert.assertEquals("selector1", queryParser.parse(" selector1  ").getContents());
        
        // check whitespace escape character
        Assert.assertEquals("C:\\\\Documents and Settings", queryParser.parse("C:\\\\Documents\\ and\\ Settings").getContents());
        
        Assert.assertEquals("an indexed phrase", queryParser.parse("an\\ indexed\\ phrase").getContents());
    }
    
    @Test
    public void testAndFilter() throws ParseException {
        LuceneQueryParser luceneParser = new LuceneQueryParser();
        Map<String,String> andNodeFilters = new HashMap<>();
        andNodeFilters.put("PROJECT", ".*");
        luceneParser.setFilters(andNodeFilters);
        
        Assert.assertEquals("[AND,a,b][posFilter: PROJECT:proj]", luceneParser.parse("PROJECT:proj a b").getContents());
        Assert.assertEquals("[OR,[AND,a,b],[AND,termx,PROJECT:A,PROJECT:B]]", luceneParser.parse("a b OR (termx PROJECT:A PROJECT:B)").getContents());
        Assert.assertEquals("[NOT,[AND,termx],[AND,a,b]][posFilter: PROJECT:A,PROJECT:B]", luceneParser.parse("termx PROJECT:A PROJECT:B NOT (a and b)")
                        .getContents());
        Assert.assertEquals("[NOT,termx,[AND,terma,PROJECT:A,PROJECT:B]]", luceneParser.parse("termx NOT (terma PROJECT:A PROJECT:B)").getContents());
        Assert.assertEquals("[NOT,termx,[NOT,terma,[AND,test2,PROJECT:A,PROJECT:B]]]", luceneParser.parse("termx NOT (terma NOT test2 PROJECT:A PROJECT:B)")
                        .getContents());
        Assert.assertEquals("[NOT,termx,PROJECT:A,PROJECT:B]", luceneParser.parse("termx NOT PROJECT:A NOT PROJECT:B").getContents());
        Assert.assertEquals("[NOT,termx,PROJECT:A,FIELDA:B]", luceneParser.parse("termx NOT PROJECT:A NOT FIELDA:B").getContents());
        Assert.assertEquals("[NOT,termx,FIELDA:B,PROJECT:A]", luceneParser.parse("termx NOT FIELDA:B NOT PROJECT:A").getContents());
        Assert.assertEquals("[NOT,termx,[NOT,terma,[AND,test2,PROJECT:A,PROJECT:B]]]", luceneParser.parse("termx NOT (terma NOT test2 PROJECT:A PROJECT:B)")
                        .getContents());
        Assert.assertEquals("[NOT,[AND,FIELD:VALUE],[AND,test2,PROJECT:A,PROJECT:B]][posFilter: PROJECT:A]",
                        luceneParser.parse("(PROJECT:A AND FIELD:VALUE) NOT (test2 PROJECT:A PROJECT:B)").getContents());
    }
    
    @Test
    public void testRanges() throws ParseException {
        LuceneQueryParser luceneParser = new LuceneQueryParser();
        Assert.assertEquals("PROJECT:{lower TO upper}", luceneParser.parse("PROJECT:{lower TO upper}").getContents());
        Assert.assertEquals("PROJECT:{lo*wer TO upper}", luceneParser.parse("PROJECT:{lo\\*wer TO upper}").getContents());
        Assert.assertEquals("PROJECT:{lo*wer TO upper}", luceneParser.parse("PROJECT:{lo*wer TO upper}").getContents());
    }
    
    @Test
    public void testSlopQuery() throws ParseException {
        LuceneQueryParser luceneParser = new LuceneQueryParser();
        
        Assert.assertEquals("[WITHIN10,quick,brown,fox]", luceneParser.parse("\"quick brown fox\"~10").getContents());
        Assert.assertEquals("quick~5", luceneParser.parse("quick\\~5").getContents());
    }
    
    @Test
    public void testMultiWordPhraseCompatability() throws ParseException {
        LuceneQueryParser luceneParser = new LuceneQueryParser();
        
        Assert.assertEquals("alpha beta", luceneParser.parse("alpha\\ beta").getContents());
        Assert.assertEquals("alpha beta", luceneParser.parse("\"alpha\\ beta\"").getContents());
        Assert.assertEquals("[AND,alpha beta,gamma]", luceneParser.parse("\"alpha\\ beta\" gamma").getContents());
        Assert.assertEquals("[ADJ1,alpha beta,gamma]", luceneParser.parse("\"alpha\\ beta gamma\"").getContents());
    }
    
    @Test
    public void testFunctions() throws ParseException {
        LuceneQueryParser luceneParser = new LuceneQueryParser();
        
        // parameters to functions do not have to be escaped
        Assert.assertEquals("[AND,fielda:selectora][posFilter: filter(true, AND, field, test[abc]*.*)]",
                        luceneParser.parse("fielda:selectora #include(field, test[abc]*.*)").getContents());
        Assert.assertEquals("[AND,fielda:selectora][posFilter: filter(true, AND, field, test[abc]\\.*.*)]",
                        luceneParser.parse("fielda:selectora #include(field, test[abc]\\.*.*)").getContents());
        Assert.assertEquals("[AND,fielda:selectora][posFilter: filter(false, AND, field, test[abc]\\.*.*)]",
                        luceneParser.parse("fielda:selectora #exclude(field, test[abc]\\.*.*)").getContents());
        Assert.assertEquals("[AND,fielda:selectora][posFilter: filter(false, AND, nullfield, .+)]", luceneParser.parse("fielda:selectora #isnull(nullfield)")
                        .getContents());
        Assert.assertEquals("[AND,field:selector][posFilter: filter(true, AND, field, testbade\\.scape)]",
                        luceneParser.parse("field:selector AND #include(field, testbade\\.scape)").getContents());
        Assert.assertEquals("[AND,field:selector][posFilter: filter(true, AND, field, testbade\\.scape)]",
                        luceneParser.parse("field:selector AND #text(field, testbade\\.scape)").getContents());
    }
    
    @Test
    public void testUnfieldedFunctions() throws ParseException {
        LuceneQueryParser luceneParser = new LuceneQueryParser();
        
        // parameters to functions do not have to be escaped
        Assert.assertEquals("[AND,field:selector][posFilter: filter(true, AND, _ANYFIELD_, testbade\\.scape)]",
                        luceneParser.parse("field:selector AND #include(testbade\\.scape)").getContents());
        Assert.assertEquals("[AND,field:selector][posFilter: filter(false, AND, _ANYFIELD_, testbade\\.scape)]",
                        luceneParser.parse("field:selector AND #exclude(testbade\\.scape)").getContents());
        Assert.assertEquals("[AND,field:selector][posFilter: filter(true, AND, _ANYFIELD_, testbade\\.scape)]",
                        luceneParser.parse("field:selector AND #text(testbade\\.scape)").getContents());
    }
    
    @Test(expected = ParseException.class)
    public void testFunctionTooDeep1() throws ParseException {
        LuceneQueryParser luceneParser = new LuceneQueryParser();
        luceneParser.parse("fielda:selectora OR (fieldb:selectorb AND #isnull(nullfield))");
    }
    
    @Test(expected = ParseException.class)
    public void testFunctionTooDeep2() throws ParseException {
        LuceneQueryParser luceneParser = new LuceneQueryParser();
        luceneParser.parse("fielda:selectora OR (fieldb:selectorb AND #isnull(nullfield)) OR fieldc:selectorc");
    }
    
    @Test(expected = ParseException.class)
    public void testFunctionWrongDepth1() throws ParseException {
        LuceneQueryParser luceneParser = new LuceneQueryParser();
        luceneParser.parse("#isnotnull(nonnullfield)");
    }
    
    @Test(expected = ParseException.class)
    public void testFunctionWrongDepth2() throws ParseException {
        LuceneQueryParser luceneParser = new LuceneQueryParser();
        luceneParser.parse("#include(field, test[abc]*.*)");
    }
    
    @Test(expected = ParseException.class)
    public void testFunctionInsufficientTerms() throws ParseException {
        LuceneQueryParser luceneParser = new LuceneQueryParser();
        luceneParser.parse("#isnull(field) #include(field, test[abc]*.*)");
    }
    
    /**
     * The include is now allowed within an or as it could be pushed down or handled by a full table scan. This is no longer an error.
     * 
     * @Test(expected = ParseException.class) public void testFunctionWrongStructure() throws ParseException { LuceneQueryParser luceneParser = new
     *                LuceneQueryParser(); luceneParser.parse("field:selector OR #include(field, test[abc]*.*)"); }
     */
    
    @Test
    public void testFunctionEscaping1() throws ParseException {
        LuceneQueryParser luceneParser = new LuceneQueryParser();
        Assert.assertEquals("[AND,field:selector][posFilter: filter(true, AND, field, fn(xxx,yyy))]",
                        luceneParser.parse("field:selector AND #include(field, fn\\(xxx\\,yyy\\))").getContents());
    }
    
    @Test(expected = ParseException.class)
    public void testFunctionEscaping2() throws ParseException {
        LuceneQueryParser luceneParser = new LuceneQueryParser();
        luceneParser.parse("field:selector OR #include(field, testbade\\.scape");
    }
    
    @Test
    public void testFunctionCapitalization() throws ParseException {
        LuceneQueryParser luceneParser = new LuceneQueryParser();
        Assert.assertEquals("[AND,field:selector][posFilter: filter(true, AND, field1, selector1, field2, selector2)]",
                        luceneParser.parse("field:selector AND #include(and, field1, selector1, field2, selector2)").getContents());
        Assert.assertEquals("[AND,field:selector][posFilter: filter(true, AND, field1, selector1, field2, selector2)]",
                        luceneParser.parse("field:selector AND #INCLUDE(AND, field1, selector1, field2, selector2)").getContents());
    }
    
    @Test
    public void testSpaceAfterColon() throws ParseException {
        LuceneQueryParser luceneParser = new LuceneQueryParser();
        Assert.assertEquals("[AND,field1:selector1,field2:selector2]", luceneParser.parse("field1: selector1 field2: selector2").getContents());
    }
    
    @Test
    public void testSmartQuoteReplacement() throws ParseException {
        LuceneQueryParser luceneParser = new LuceneQueryParser();
        String s = "\u0093see jane run\u0094";
        Assert.assertEquals("[ADJ2,see,jane,run]", luceneParser.parse(s).getContents());
    }
    
    @Test
    public void testParseSpaces() throws ParseException {
        LuceneQueryParser luceneParser = new LuceneQueryParser();
        Assert.assertEquals("FIELD:  ", luceneParser.parse("FIELD:\\ \\ ").getContents());
        Assert.assertEquals("FIELD:  TEST", luceneParser.parse("FIELD:\\ \\ TEST").getContents());
        Assert.assertEquals("FIELD:TEST  ", luceneParser.parse("FIELD:TEST\\ \\ ").getContents());
    }
}
