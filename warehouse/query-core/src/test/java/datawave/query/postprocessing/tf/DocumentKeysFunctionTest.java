package datawave.query.postprocessing.tf;

import com.google.common.collect.Sets;
import datawave.data.type.LcNoDiacriticsType;
import datawave.query.attributes.Content;
import datawave.query.attributes.Document;
import datawave.query.attributes.PreNormalizedAttribute;
import datawave.query.attributes.TypeAttribute;
import datawave.query.jexl.JexlASTHelper;
import org.apache.accumulo.core.data.Key;
import org.junit.Test;

import java.util.Collections;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class DocumentKeysFunctionTest {
    
    private final Key docKey = new Key("row", "datatype\0uid");
    private final Key docKey1 = new Key("row", "datatype\0uid.1");
    private final Key docKey2 = new Key("row", "datatype\0uid.2");
    private final Key docKey3 = new Key("row", "datatype\0uid.3");
    private final Key docKey4 = new Key("row", "datatype\0uid.4");
    private final Key docKey5 = new Key("row", "datatype\0uid.5");
    private final Key docKey6 = new Key("row", "datatype\0uid.6");
    private final Key docKey7 = new Key("row", "datatype\0uid.7");
    private final Set<Key> docKeys = Sets.newHashSet(docKey, docKey1, docKey2, docKey3, docKey4, docKey5, docKey6, docKey7);
    
    @Test
    public void testTldAndEvenChildren() throws Exception {
        // top level document + all event children
        String query = "content:phrase(FOO, termOffsetMap, 'bar', 'baz') && FOO == 'bar' && FOO == 'baz'";
        Set<Key> expected = Sets.newHashSet(docKey, docKey2, docKey4);
        test(query, expected);
    }
    
    @Test
    public void testTldAndOddChildren() throws Exception {
        // top level document + all odd children
        String query = "content:phrase(FOO, termOffsetMap, 'bar', 'biz') && FOO == 'bar' && FOO == 'biz'";
        Set<Key> expected = Sets.newHashSet(docKey, docKey1, docKey3, docKey5);
        test(query, expected);
    }
    
    // current implementation just make sure keys exist
    @Test
    public void testTldAndNonIntersectingChildren() throws Exception {
        // top level document. even and odd children do not intersect
        String query = "content:phrase(FOO, termOffsetMap, 'baz', 'biz') && FOO == 'baz' && FOO == 'biz'";
        test(query, Collections.singleton(docKey));
    }
    
    @Test
    public void testTldWithNegatedPhrase() throws Exception {
        // bar+baz is all even children
        // biz+biz is all odd children
        // combined search space should be tld + all children
        String query = "(content:phrase(FOO, termOffsetMap, 'bar', 'baz') && FOO == 'bar' && FOO == 'baz') &&"
                        + "!(content:phrase(FOO, termOffsetMap, 'biz', 'biz') && FOO == 'biz' && FOO == 'biz') ";
        Set<Key> expected = Sets.newHashSet(docKey, docKey1, docKey2, docKey3, docKey4, docKey5, docKey6, docKey7);
        test(query, expected);
    }
    
    @Test
    public void testOnlyNegatedPhrase() throws Exception {
        // even though FOO:baz only hits in the TLD and child 2 and 4, still need to search all doc keys
        String query = "!(content:phrase(FOO, termOffsetMap, 'baz', 'baz') && FOO == 'baz')";
        Set<Key> expected = Sets.newHashSet(docKey, docKey1, docKey2, docKey3, docKey4, docKey5, docKey6, docKey7);
        test(query, expected);
    }
    
    @Test
    public void testFieldAsIdentifier() throws Exception {
        // top level document + all event children
        String query = "content:phrase($FOO, termOffsetMap, 'bar', 'baz') && FOO == 'bar' && FOO == 'baz'";
        Set<Key> expected = Sets.newHashSet(docKey, docKey2, docKey4);
        test(query, expected);
    }
    
    private void test(String query, Set<Key> expectedDocKeys) throws Exception {
        TermFrequencyConfig config = new TermFrequencyConfig();
        config.setScript(JexlASTHelper.parseAndFlattenJexlQuery(query));
        
        DocumentKeysFunction function = new DocumentKeysFunction(config);
        
        Set<Key> filteredKeys = function.getDocKeys(createDocument(), docKeys);
        assertEquals(expectedDocKeys, filteredKeys);
    }
    
    private Document createDocument() {
        Document d = new Document();
        
        // FOO == 'bar' hits in TLD and all five children
        d.put("FOO", new PreNormalizedAttribute("bar", new Key("row", "datatype\0uid"), true));
        d.put("FOO", new PreNormalizedAttribute("bar", new Key("row", "datatype\0uid.1"), true));
        d.put("FOO", new PreNormalizedAttribute("bar", new Key("row", "datatype\0uid.2"), true));
        d.put("FOO", new PreNormalizedAttribute("bar", new Key("row", "datatype\0uid.3"), true));
        d.put("FOO", new PreNormalizedAttribute("bar", new Key("row", "datatype\0uid.4"), true));
        d.put("FOO", new PreNormalizedAttribute("bar", new Key("row", "datatype\0uid.5"), true));
        
        // FOO == 'baz' hits in TLD an all EVEN children
        d.put("FOO", new PreNormalizedAttribute("baz", new Key("row", "datatype\0uid"), true));
        d.put("FOO", new PreNormalizedAttribute("baz", new Key("row", "datatype\0uid.2"), true));
        d.put("FOO", new PreNormalizedAttribute("baz", new Key("row", "datatype\0uid.4"), true));
        
        // FOO == 'biz' hits in TLD and all ODD children
        d.put("FOO", new PreNormalizedAttribute("biz", new Key("row", "datatype\0uid"), true));
        d.put("FOO", new PreNormalizedAttribute("biz", new Key("row", "datatype\0uid.1"), true));
        d.put("FOO", new PreNormalizedAttribute("biz", new Key("row", "datatype\0uid.3"), true));
        d.put("FOO", new PreNormalizedAttribute("biz", new Key("row", "datatype\0uid.5"), true));
        d.put("FOO", new Content("biz", new Key("row", "datatype\0uid.6"), true));
        d.put("FOO", new TypeAttribute<>(new LcNoDiacriticsType("biz"), new Key("row", "datatype\0uid.7"), true));
        
        // TODO -- different column visibilities
        
        return d;
    }
    
}
