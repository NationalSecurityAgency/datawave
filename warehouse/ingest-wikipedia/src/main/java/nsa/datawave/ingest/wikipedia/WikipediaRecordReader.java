package nsa.datawave.ingest.wikipedia;

import com.google.common.collect.Maps;
import nsa.datawave.data.hash.UID;
import nsa.datawave.ingest.config.IngestConfigurationFactory;
import nsa.datawave.ingest.config.RawRecordContainerImpl;
import nsa.datawave.ingest.data.RawDataErrorNames;
import nsa.datawave.ingest.data.RawRecordContainer;
import nsa.datawave.ingest.data.Type;
import nsa.datawave.util.time.DateHelper;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.analysis.wikipedia.WikipediaTokenizer;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;

/**
 * 
 */
public class WikipediaRecordReader extends AggregatingRecordReader {
    private static final Logger log = Logger.getLogger(WikipediaRecordReader.class);
    
    protected static final String BEGIN = "<page>";
    protected static final String END = "</page>";
    protected static final String PAGE_ELEMENT = "page";
    protected static final String TIMESTAMP_ELEMENT = "timestamp";
    protected static final String WIKI = "wiki-", WIKTIONARY = "wiktionary-";
    protected static final String[] FILE_NAME_PREFIXES = new String[] {WIKI, WIKTIONARY};
    protected static final String DEFAULT_SECURITY_MARKING = "PUBLIC";
    
    private static DocumentBuilderFactory factory = configureFactory();
    
    protected WikipediaHelper wikiHelper = new WikipediaHelper();
    protected WikipediaTokenizer wikiTokenizer = null;
    protected CharTermAttribute termAttr = null;
    protected TypeAttribute typeAttr = null;
    protected HashMap<String,Type> wikipediaTypeRegistry = Maps.newHashMap();
    
    private RawRecordContainerImpl event = new RawRecordContainerImpl();
    private DocumentBuilder parser = null;
    
    private static DocumentBuilderFactory configureFactory() {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        try {
            factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", false);
            factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
            factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
            factory.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
            factory.setXIncludeAware(false);
            factory.setExpandEntityReferences(false);
            return factory;
            
        } catch (ParserConfigurationException ex) {
            log.error("Unable to configure factory", ex);
            return null;
        }
    }
    
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
        // Set the START and STOP tokens for the AggregatingRecordReader
        context.getConfiguration().set(AggregatingRecordReader.START_TOKEN, BEGIN);
        context.getConfiguration().set(AggregatingRecordReader.END_TOKEN, END);
        context.getConfiguration().set(AggregatingRecordReader.RETURN_PARTIAL_MATCHES, Boolean.toString(true));
        
        super.initialize(split, context);
        
        helper.setup(context.getConfiguration());
        
        this.wikiHelper = (WikipediaHelper) this.helper;
        
        if (split instanceof FileSplit) {
            FileSplit fs = (FileSplit) split;
            Path p = fs.getPath();
            rawFileName = p.getName();
            
            if (log.isDebugEnabled()) {
                log.debug("FileSplit Info: ");
                log.debug("Start: " + fs.getStart());
                log.debug("Length: " + fs.getLength());
                log.debug("Locations: " + Arrays.toString(fs.getLocations()));
                log.debug("Path: " + fs.getPath());
            }
        } else {
            throw new IOException("Input Split unhandled.");
        }
    }
    
    @Override
    public WikipediaHelper createHelper(Configuration conf) {
        return wikiHelper;
    }
    
    @Override
    public void initializeEvent(Configuration conf) throws IOException {
        super.initializeEvent(conf);
        markingsHelper = IngestConfigurationFactory.getIngestConfiguration().getMarkingsHelper(conf, helper.getType());
        
        try {
            this.parser = factory.newDocumentBuilder();
        } catch (ParserConfigurationException e) {
            throw new IOException("Error instantiating DocumentBuilder", e);
        }
    }
    
    @Override
    public RawRecordContainer getEvent() {
        event.clear();
        
        event.setDataType(helper.getType());
        event.setRawFileName(rawFileName);
        
        try {
            event.setRawRecordNumber(getCurrentKey().get());
        } catch (Exception e) {
            throw new RuntimeException("Unable to get current key", e);
        }
        
        // get security marking set in the config, otherwise default to PUBLIC
        if (markingsHelper != null) {
            event.setSecurityMarkings(markingsHelper.getDefaultMarkings());
        }
        if (event.getVisibility() == null) {
            event.setVisibility(new ColumnVisibility(DEFAULT_SECURITY_MARKING));
        }
        
        Text rawPageText = getCurrentValue();
        
        this.parser.reset();
        String data = rawPageText.toString().trim();
        
        event.setRawData(data.getBytes());
        
        try {
            StringReader reader = new StringReader(data);
            InputSource source = new InputSource(reader);
            parser.parse(source);
        } catch (Exception e) {
            log.info("Could not parse xml: " + data);
            event.addError(RawDataErrorNames.INVALID_XML);
        }
        
        event.setId(UID.builder().newId(data.getBytes(), new Date()));
        
        updateEventTypeInformation(event);
        
        updateEventDate(event);
        
        return event;
    }
    
    /**
     * We don't want to call every single wikipedia event "wikipedia" but to use the language and type of the dump to better classify the event
     * 
     * @param event
     */
    protected void updateEventTypeInformation(RawRecordContainer event) {
        String rawFileName = event.getRawFileName();
        
        // Try to extrapolate the type based on filename
        if (StringUtils.isBlank(rawFileName)) {
            throw new IllegalArgumentException("Could not extract type information from an empty filename. " + event);
        }
        
        for (String searchPrefix : FILE_NAME_PREFIXES) {
            int index = rawFileName.indexOf(searchPrefix);
            
            if (-1 != index) {
                String newType = rawFileName.substring(0, index + searchPrefix.length() - 1);
                
                Type t = null;
                if (wikipediaTypeRegistry.containsKey(newType)) {
                    t = wikipediaTypeRegistry.get(newType);
                } else {
                    Type originalType = event.getDataType();
                    t = new Type(originalType.typeName(), newType, originalType.getHelperClass(), originalType.getReaderClass(),
                                    originalType.getDefaultDataTypeHandlers(), originalType.getFilterPriority(), originalType.getDefaultDataTypeFilters());
                    
                    wikipediaTypeRegistry.put(newType, t);
                }
                
                event.setDataType(t);
                
                return;
            }
        }
        
        throw new IllegalStateException("Could not assign a language/wikipedia-type remapping for datatype from '" + rawFileName + "'");
    }
    
    protected void updateEventDate(RawRecordContainer event) {
        String rawFileName = event.getRawFileName();
        
        if (StringUtils.isBlank(rawFileName)) {
            throw new IllegalArgumentException("Could not extract date from an empty filename. " + event);
        }
        
        int startIndex = rawFileName.indexOf('-');
        if (-1 == startIndex) {
            throw new IllegalArgumentException("Could not extract date from filename " + rawFileName);
        }
        
        startIndex++;
        
        int endIndex = rawFileName.indexOf('-', startIndex);
        if (-1 == endIndex) {
            throw new IllegalArgumentException("Could not extract date from filename " + rawFileName);
        }
        
        String date = rawFileName.substring(startIndex, endIndex);
        
        try {
            Date eventDate = DateHelper.parse(date);
            event.setDate(eventDate.getTime());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Could not parse date from filename " + date);
        }
    }
    
}
