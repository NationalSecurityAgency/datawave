package datawave.query.rewrite.jexl.visitors;

import com.google.common.collect.Sets;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collections;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.jexl.visitors.PushdownLargeFieldedListsVisitor;
import datawave.query.jexl.visitors.TreeEqualityVisitor;
import datawave.query.jexl.visitors.TreeFlatteningRebuildingVisitor;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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
        String rewritten = JexlStringBuildingVisitor.buildQuery(PushdownLargeFieldedListsVisitor.pushdown(conf,
                        TreeFlatteningRebuildingVisitor.flatten(JexlASTHelper.parseJexlQuery("FOO == 'BAR'")), null, null));
        Assert.assertEquals("FOO == 'BAR'", rewritten);
    }
    
}
