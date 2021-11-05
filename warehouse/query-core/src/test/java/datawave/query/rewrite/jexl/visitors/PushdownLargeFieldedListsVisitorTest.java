package datawave.query.rewrite.jexl.visitors;

import com.google.common.collect.Sets;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.jexl.visitors.PushdownLargeFieldedListsVisitor;
import datawave.query.jexl.visitors.TreeFlatteningRebuildingVisitor;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class PushdownLargeFieldedListsVisitorTest {
    protected ShardQueryConfiguration conf = null;
    
    @Before
    public void setupConfiguration() {
        conf = new ShardQueryConfiguration();
        conf.setMaxOrExpansionThreshold(3);
        conf.setMaxOrExpansionFstThreshold(100);
        conf.setIndexedFields(Sets.newHashSet("FOO", "BAR", "FOOBAR"));
    }
    
    @Test
    public void testSimpleExpression() throws Throwable {
        testSimple("FOO == 'BAR'", "FOO == 'BAR'");
    }
    
    @Test
    public void testMultipleExpression() throws Throwable {
        testSimple("FOO == 'BAR' || FOO == 'FOO' || BAR == 'FOO'", "BAR == 'FOO' || FOO == 'BAR' || FOO == 'FOO'");
    }
    
    @Test
    public void testPushdown() throws Throwable {
        testSingleId("FOO == 'BAR' || FOO == 'FOO' || FOO == 'FOOBAR'", "((_List_ = true) && ((id = '",
                        "') && (field = 'FOO') && (params = '{\"values\":[\"BAR\",\"FOO\",\"FOOBAR\"]}')))");
    }
    
    @Test
    public void testEscapedValues() throws Throwable {
        testSingleId("FOO == 'BAR' || FOO == 'FOO' || FOO == 'FOO,BAR'", "((_List_ = true) && ((id = '",
                        "') && (field = 'FOO') && (params = '{\"values\":[\"BAR\",\"FOO\",\"FOO,BAR\"]}')))");
    }
    
    @Test
    public void testPushdownPartial() throws Throwable {
        testSingleId("FOO == 'BAR' || BAR == 'BAR' || FOO == 'FOO' || BAR == 'FOO' || FOO == 'FOOBAR'",
                        "BAR == 'BAR' || BAR == 'FOO' || ((_List_ = true) && ((id = '",
                        "') && (field = 'FOO') && (params = '{\"values\":[\"BAR\",\"FOO\",\"FOOBAR\"]}')))");
    }
    
    @Test
    public void testPushdownMultiple() throws Throwable {
        testDoubleId("FOO == 'BAR' || BAR == 'BAR' || FOO == 'FOO' || BAR == 'FOO' || FOO == 'FOOBAR' || BAR == 'FOOBAR'", "((_List_ = true) && ((id = '",
                        "') && (field = 'BAR') && (params = '{\"values\":[\"BAR\",\"FOO\",\"FOOBAR\"]}'))) || ((_List_ = true) && ((id = '",
                        "') && (field = 'FOO') && (params = '{\"values\":[\"BAR\",\"FOO\",\"FOOBAR\"]}')))");
    }
    
    @Test
    public void testPushdownIgnoreAnyfield() throws Throwable {
        testSingleId("FOO == 'BAR' || _ANYFIELD_ == 'BAR' || FOO == 'FOO' || _ANYFIELD_ == 'FOO' || FOO == 'FOOBAR' || _ANYFIELD_ == 'FOOBAR'",
                        "((_List_ = true) && ((id = '",
                        "') && (field = 'FOO') && (params = '{\"values\":[\"BAR\",\"FOO\",\"FOOBAR\"]}'))) || _ANYFIELD_ == 'BAR' || _ANYFIELD_ == 'FOO' || _ANYFIELD_ == 'FOOBAR'");
    }
    
    @Test
    public void testPushdownIgnoreOtherNodes() throws Throwable {
        conf.setMaxOrExpansionThreshold(1);
        testSingleId("f:includeRegex(FOO, 'blabla') || FOO == 'BAR' || _ANYFIELD_ == 'BAR' || FOO == 'FOO' || _ANYFIELD_ == 'FOO' || FOO == 'FOOBAR' || _ANYFIELD_ == 'FOOBAR'",
                        "f:includeRegex(FOO, 'blabla') || ((_List_ = true) && ((id = '",
                        "') && (field = 'FOO') && (params = '{\"values\":[\"BAR\",\"FOO\",\"FOOBAR\"]}'))) || _ANYFIELD_ == 'BAR' || _ANYFIELD_ == 'FOO' || _ANYFIELD_ == 'FOOBAR'");
    }
    
    @Test
    public void testPushdownComplex() throws Throwable {
        testDoubleId("f:includeRegex(FOO, 'blabla') && X == 'Y' && (FOO == 'BAR' || BAR == 'BAR' || FOO == 'FOO' || BAR == 'FOO' || FOO == 'FOOBAR' || BAR == 'FOOBAR') && !(Y == 'X')",
                        "f:includeRegex(FOO, 'blabla') && X == 'Y' && (((_List_ = true) && ((id = '",
                        "') && (field = 'BAR') && (params = '{\"values\":[\"BAR\",\"FOO\",\"FOOBAR\"]}'))) || ((_List_ = true) && ((id = '",
                        "') && (field = 'FOO') && (params = '{\"values\":[\"BAR\",\"FOO\",\"FOOBAR\"]}')))) && !(Y == 'X')");
        
    }
    
    @Test
    public void testPushdownFst() throws Throwable {
        setupFst("FOO == 'BAR' || FOO == 'FOO' ||  FOO == 'FOOBAR'", "((_List_ = true) && ((id = '", "') && (field = 'FOO') && (params = '{\"fstURI\":\"");
    }
    
    private void testSimple(String query, String expected) throws Throwable {
        testSimple(query, expected, conf); // pass in the default shard query config
    }
    
    private void testSimple(String query, String expected, ShardQueryConfiguration config) throws Throwable {
        String rewritten = JexlStringBuildingVisitor.buildQuery(PushdownLargeFieldedListsVisitor.pushdown(config,
                        TreeFlatteningRebuildingVisitor.flatten(JexlASTHelper.parseJexlQuery(query)), null, null));
        assertEquals("EXPECTED: " + expected + "\nACTUAL: " + rewritten, expected, rewritten);
    }
    
    private void testSingleId(String query, String left, String right) throws Throwable {
        String rewritten = JexlStringBuildingVisitor.buildQuery(PushdownLargeFieldedListsVisitor.pushdown(conf,
                        TreeFlatteningRebuildingVisitor.flatten(JexlASTHelper.parseJexlQuery(query)), null, null));
        String id = rewritten.substring(rewritten.indexOf("id = '") + 6, rewritten.indexOf("') && (field"));
        assertEquals(left + id + right, rewritten);
    }
    
    private void testDoubleId(String query, String left1, String right1, String left2) throws Throwable {
        String rewritten = JexlStringBuildingVisitor.buildQuery(PushdownLargeFieldedListsVisitor.pushdown(conf,
                        TreeFlatteningRebuildingVisitor.flatten(JexlASTHelper.parseJexlQuery(query)), null, null));
        String id1 = rewritten.substring(rewritten.indexOf("id = '") + 6, rewritten.indexOf("') && (field"));
        String id2 = rewritten.substring(rewritten.lastIndexOf("id = '") + 6, rewritten.lastIndexOf("') && (field"));
        assertEquals(left1 + id1 + right1 + id2 + left2, rewritten);
    }
    
    private void testFst(String query, String left, String right, FileSystem fileSystem, URI hdfsCacheURI) throws Throwable {
        String rewritten = JexlStringBuildingVisitor.buildQuery(PushdownLargeFieldedListsVisitor.pushdown(conf,
                        TreeFlatteningRebuildingVisitor.flatten(JexlASTHelper.parseJexlQuery(query)), fileSystem, hdfsCacheURI.toString()));
        String id = rewritten.substring(rewritten.indexOf("id = '") + 6, rewritten.indexOf("') && (field"));
        assertEquals(left + id + right + hdfsCacheURI + "/PushdownLargeFileFst.1.fst\"}')))", rewritten);
    }
    
    private void setupFst(String query, String left, String right) throws Throwable {
        conf.setMaxOrExpansionFstThreshold(3);
        
        final URI hdfsCacheURI;
        final FileSystem fileSystem;
        File tmpDir = null;
        try {
            URL hadoopConfig = this.getClass().getResource("/testhadoop.config");
            Configuration hadoopConf = new Configuration();
            hadoopConf.addResource(hadoopConfig);
            
            File tmpFile = File.createTempFile("Ivarator", ".cache");
            tmpDir = new File(tmpFile.getAbsoluteFile() + File.separator);
            tmpFile.delete();
            tmpDir.mkdir();
            
            hdfsCacheURI = new URI("file:" + tmpDir.getPath());
            fileSystem = FileSystem.get(hdfsCacheURI, hadoopConf);
            
            conf.setHdfsSiteConfigURLs(hadoopConfig.toExternalForm());
            conf.setIvaratorCacheDirConfigs(Collections.singletonList(new IvaratorCacheDirConfig(hdfsCacheURI.toString())));
            testFst(query, left, right, fileSystem, hdfsCacheURI);
            
        } catch (MalformedURLException e) {
            throw new IllegalStateException("Unable to load hadoop configuration", e);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to create hadoop file system", e);
        } catch (URISyntaxException e) {
            throw new IllegalStateException("Invalid hdfs cache dir URI", e);
        } finally {
            if (tmpDir != null) {
                FileUtils.deleteDirectory(tmpDir);
            }
        }
    }
}
