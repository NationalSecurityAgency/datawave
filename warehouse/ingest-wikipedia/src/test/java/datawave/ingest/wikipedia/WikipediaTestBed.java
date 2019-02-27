package datawave.ingest.wikipedia;

import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;

import datawave.data.type.LcNoDiacriticsType;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.DataTypeHelper;
import datawave.ingest.data.config.ingest.BaseIngestHelper;
import datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler;
import datawave.ingest.mapreduce.handler.tokenize.ExtendedContentIndexingColumnBasedHandler;
import datawave.policy.ExampleIngestPolicyEnforcer;

import datawave.util.TableName;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.Assert;
import org.junit.Before;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 * 
 */
public class WikipediaTestBed {
    protected Configuration conf = null;
    protected TaskAttemptContext ctx = null;
    protected InputSplit split = null;
    protected Multimap<String,String> expectedRawFieldsRecord1 = HashMultimap.create(), expectedRawFieldsRecord2 = HashMultimap.create(),
                    expectedNormalizedFieldsRecord1 = HashMultimap.create(), expectedNormalizedFieldsRecord2 = HashMultimap.create();
    
    @Before
    public void initializeInputData() throws Exception {
        
        conf = new Configuration();
        conf.set("all" + DataTypeHelper.Properties.INGEST_POLICY_ENFORCER_CLASS, ExampleIngestPolicyEnforcer.class.getName());
        
        URL url = ClassLoader.getSystemResource("config/ingest/wikipedia-config.xml");
        Assert.assertNotNull("URL to wikipedia-config.xml was null", url);
        conf.addResource(url);
        
        url = ClassLoader.getSystemResource("config/ingest/metadata-config.xml");
        Assert.assertNotNull("URL to metadata-config.xml was null", url);
        conf.addResource(url);
        
        conf.setInt(ShardedDataTypeHandler.NUM_SHARDS, 1);
        conf.set(ShardedDataTypeHandler.SHARD_TNAME, TableName.SHARD);
        conf.set(ShardedDataTypeHandler.SHARD_GIDX_TNAME, TableName.SHARD_INDEX);
        conf.set(ShardedDataTypeHandler.SHARD_GRIDX_TNAME, TableName.SHARD_RINDEX);
        conf.set(ShardedDataTypeHandler.METADATA_TABLE_NAME, TableName.METADATA);
        conf.set(BaseIngestHelper.DEFAULT_TYPE, LcNoDiacriticsType.class.getName());
        conf.setBoolean(ExtendedContentIndexingColumnBasedHandler.OPT_OFFLINE, true);
        
        TypeRegistry.reset();
        TypeRegistry.getInstance(conf);
        
        this.split = getSplit("/input/enwiki-20130305-pages-articles-brief.xml");
        
        this.ctx = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        
        createExpectedResults();
    }
    
    protected InputSplit getSplit(String file) throws URISyntaxException, MalformedURLException {
        URL data = WikipediaRecordReaderTest.class.getResource(file);
        if (data == null) {
            File fileObj = new File(file);
            if (fileObj.exists()) {
                data = fileObj.toURI().toURL();
            }
        }
        assertNotNull("Did not find test resource", data);
        
        File dataFile = new File(data.toURI());
        Path p = new Path(dataFile.toURI().toString());
        return new FileSplit(p, 0, dataFile.length(), null);
    }
    
    protected void createExpectedResults() {
        expectedRawFieldsRecord1.put("PAGE_TITLE", "AccessibleComputing");
        expectedRawFieldsRecord1.put("PAGE_NAMESPACE", "0");
        expectedRawFieldsRecord1.put("PAGE_ID", "10");
        expectedRawFieldsRecord1.put("PAGE_REDIRECT_TITLE", "Computer accessibility");
        expectedRawFieldsRecord1.put("REVISION_ID", "381202555");
        expectedRawFieldsRecord1.put("REVISION_PARENTID", "381200179");
        expectedRawFieldsRecord1.put("REVISION_TIMESTAMP", "2010-08-26T22:38:36Z");
        expectedRawFieldsRecord1.put("CONTRIBUTOR_USERNAME", "OlEnglish");
        expectedRawFieldsRecord1.put("CONTRIBUTOR_ID", "7181920");
        expectedRawFieldsRecord1
                        .put("REVISION_COMMENT",
                                        "[[Help:Reverting|Reverted]] edits by [[Special:Contributions/76.28.186.133|76.28.186.133]] ([[User talk:76.28.186.133|talk]]) to last version by Gurch");
        expectedRawFieldsRecord1.put("REVISION_TEXT_SPACE", "preserve");
        expectedRawFieldsRecord1.put("REVISION_SHA1", "lo15ponaybcg2sf49sstw9gdjmdetnk");
        expectedRawFieldsRecord1.put("REVISION_MODEL", "wikitext");
        expectedRawFieldsRecord1.put("REVISION_FORMAT", "text/x-wiki");
        expectedRawFieldsRecord1.put("LANGUAGE", "ENGLISH");
        
        expectedNormalizedFieldsRecord1.put("PAGE_TITLE", "accessiblecomputing");
        expectedNormalizedFieldsRecord1.put("PAGE_NAMESPACE", "+AE0");
        expectedNormalizedFieldsRecord1.put("PAGE_ID", "+bE1");
        expectedNormalizedFieldsRecord1.put("PAGE_REDIRECT_TITLE", "computer accessibility");
        expectedNormalizedFieldsRecord1.put("REVISION_ID", "+iE3.81202555");
        expectedNormalizedFieldsRecord1.put("REVISION_PARENTID", "+iE3.81200179");
        expectedNormalizedFieldsRecord1.put("REVISION_TIMESTAMP", "2010-08-26t22:38:36z");
        expectedNormalizedFieldsRecord1.put("CONTRIBUTOR_USERNAME", "olenglish");
        expectedNormalizedFieldsRecord1.put("CONTRIBUTOR_ID", "+gE7.18192");
        expectedNormalizedFieldsRecord1
                        .put("REVISION_COMMENT",
                                        "[[help:reverting|reverted]] edits by [[special:contributions/76.28.186.133|76.28.186.133]] ([[user talk:76.28.186.133|talk]]) to last version by gurch");
        expectedNormalizedFieldsRecord1.put("REVISION_TEXT_SPACE", "preserve");
        expectedNormalizedFieldsRecord1.put("REVISION_SHA1", "lo15ponaybcg2sf49sstw9gdjmdetnk");
        expectedNormalizedFieldsRecord1.put("REVISION_MODEL", "wikitext");
        expectedNormalizedFieldsRecord1.put("REVISION_FORMAT", "text/x-wiki");
        expectedNormalizedFieldsRecord1.put("LANGUAGE", "english");
        
        expectedRawFieldsRecord2.put("PAGE_TITLE", "Anarchism");
        expectedRawFieldsRecord2.put("PAGE_NAMESPACE", "0");
        expectedRawFieldsRecord2.put("PAGE_ID", "12");
        expectedRawFieldsRecord2.put("REVISION_ID", "541785980");
        expectedRawFieldsRecord2.put("REVISION_PARENTID", "541640576");
        expectedRawFieldsRecord2.put("REVISION_TIMESTAMP", "2013-03-02T21:01:50Z");
        expectedRawFieldsRecord2.put("CONTRIBUTOR_USERNAME", "Altaïr");
        expectedRawFieldsRecord2.put("CONTRIBUTOR_ID", "15232315");
        expectedRawFieldsRecord2.put("REVISION_COMMENT", "/* Origins */  Fix.");
        expectedRawFieldsRecord2.put("REVISION_TEXT_SPACE", "preserve");
        expectedRawFieldsRecord2.put("REVISION_SHA1", "ls0ordqhjufi32fc3qo9oioa37seld5");
        expectedRawFieldsRecord2.put("REVISION_MODEL", "wikitext");
        expectedRawFieldsRecord2.put("REVISION_FORMAT", "text/x-wiki");
        expectedRawFieldsRecord2.put("LANGUAGE", "ENGLISH");
        
        expectedNormalizedFieldsRecord2.put("PAGE_TITLE", "anarchism");
        expectedNormalizedFieldsRecord2.put("PAGE_NAMESPACE", "+AE0");
        expectedNormalizedFieldsRecord2.put("PAGE_ID", "+bE1.2");
        expectedNormalizedFieldsRecord2.put("REVISION_ID", "+iE5.4178598");
        expectedNormalizedFieldsRecord2.put("REVISION_PARENTID", "+iE5.41640576");
        expectedNormalizedFieldsRecord2.put("REVISION_TIMESTAMP", "2013-03-02t21:01:50z");
        expectedNormalizedFieldsRecord2.put("CONTRIBUTOR_USERNAME", "altair");
        expectedNormalizedFieldsRecord2.put("CONTRIBUTOR_ID", "+hE1.5232315");
        expectedNormalizedFieldsRecord2.put("REVISION_COMMENT", "/* origins */  fix.");
        expectedNormalizedFieldsRecord2.put("REVISION_TEXT_SPACE", "preserve");
        expectedNormalizedFieldsRecord2.put("REVISION_SHA1", "ls0ordqhjufi32fc3qo9oioa37seld5");
        expectedNormalizedFieldsRecord2.put("REVISION_MODEL", "wikitext");
        expectedNormalizedFieldsRecord2.put("REVISION_FORMAT", "text/x-wiki");
        expectedNormalizedFieldsRecord2.put("LANGUAGE", "english");
    }
}
