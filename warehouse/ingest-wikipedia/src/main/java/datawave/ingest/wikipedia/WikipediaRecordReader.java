package datawave.ingest.wikipedia;

import static datawave.ingest.input.reader.LineReader.Properties.LONGLINE_NEWLINE_INCLUDED;

import java.io.IOException;
import java.io.StringReader;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.analysis.wikipedia.WikipediaTokenizer;
import org.xml.sax.InputSource;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import datawave.data.hash.UID;
import datawave.ingest.data.RawDataErrorNames;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.config.DataTypeHelper;
import datawave.ingest.input.reader.AggregatingRecordReader;
import datawave.ingest.input.reader.EventInitializer;
import datawave.ingest.input.reader.KeyReader;
import datawave.ingest.input.reader.ReaderInitializer;
import datawave.ingest.input.reader.ValueReader;
import datawave.ingest.input.reader.event.EventFixer;
import datawave.util.time.DateHelper;

public class WikipediaRecordReader extends AggregatingRecordReader {

    public static final String DEFAULT_SECURITY_MARKING = "PUBLIC";

    private static final Logger log = Logger.getLogger(WikipediaRecordReader.class);

    private ReaderDelegate delegate = null;

    public WikipediaRecordReader() {
        delegate = new ReaderDelegate();
        delegate.setEvent(INGEST_CONFIG.createRawRecordContainer());
        delegate.setEventInitializer(this::initializeEventSuperClass);
        delegate.setReaderInitializer(this::initializeSuperClass);
        delegate.setCurrentKeyReader(this::currentKey);
        delegate.setCurrentValueReader(this::currentVal);

    }

    private Text currentVal() {
        return this.getCurrentValue();
    }

    private LongWritable currentKey() {
        return this.getCurrentKey();
    }

    private void initializeSuperClass(InputSplit split, TaskAttemptContext context) throws IOException {
        super.initialize(split, context);
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

    private void initializeEventSuperClass(Configuration conf) throws IOException {
        super.initializeEvent(conf);
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
        delegate.initialize(split, context);
    }

    @Override
    public DataTypeHelper createHelper(Configuration conf) {
        return delegate.getWikipediaHelper();
    }

    @Override
    public void initializeEvent(Configuration conf) throws IOException {
        delegate.initializeEvent(conf);
    }

    @Override
    protected void setDefaultSecurityMarkings(RawRecordContainer rrc) {
        // get security marking set in the config, otherwise default to PUBLIC
        if (markingsHelper != null) {
            rrc.setSecurityMarkings(markingsHelper.getDefaultMarkings());
        }
        if (rrc.getVisibility() == null) {
            rrc.setVisibility(new ColumnVisibility(DEFAULT_SECURITY_MARKING));
        }
    }

    @Override
    public RawRecordContainer getEvent() {
        RawRecordContainer event = delegate.getEvent(this.rawFileName);
        setDefaultSecurityMarkings(event);
        return event;
    }

    /**
     * Encapsulates all the wikipedia xml RecordReader logic out-of-band from the established inheritance hierarchy so that clients can extend/inherit whatever
     * suits their needs
     */
    public static class ReaderDelegate {
        protected static final String BEGIN = "<page>";
        protected static final String END = "</page>";
        protected static final String PAGE_ELEMENT = "page";
        protected static final String TIMESTAMP_ELEMENT = "timestamp";
        protected static final String WIKI = "wiki-", WIKTIONARY = "wiktionary-";
        protected static final String[] FILE_NAME_PREFIXES = new String[] {WIKI, WIKTIONARY};

        protected static final DocumentBuilderFactory FACTORY = configureFactory();

        protected WikipediaHelper wikiHelper = new WikipediaHelper();
        protected WikipediaTokenizer wikiTokenizer = null;
        protected CharTermAttribute termAttr = null;
        protected TypeAttribute typeAttr = null;
        protected HashMap<String,Type> wikipediaTypeRegistry = Maps.newHashMap();
        protected DocumentBuilder parser = null;

        private RawRecordContainer event = null;
        private EventInitializer eventInitializer;
        private ReaderInitializer readerInitializer;
        private KeyReader currentKeyReader;
        private ValueReader currentValueReader;
        private EventFixer eventFixer = null;

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
                log.error("Unable to configure FACTORY", ex);
                return null;
            }
        }

        public void setEvent(RawRecordContainer event) {
            this.event = event;
        }

        public void setEventInitializer(EventInitializer eventInitializer) {
            this.eventInitializer = eventInitializer;
        }

        public void setReaderInitializer(ReaderInitializer readerInitializer) {
            this.readerInitializer = readerInitializer;
        }

        public void setCurrentKeyReader(KeyReader currentKeyReader) {
            this.currentKeyReader = currentKeyReader;
        }

        public void setCurrentValueReader(ValueReader currentValueReader) {
            this.currentValueReader = currentValueReader;
        }

        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
            Preconditions.checkNotNull(readerInitializer, "readerInitializer cannot be null");
            Preconditions.checkNotNull(context, "context cannot be null");

            // Set the START and STOP tokens for the AggregatingRecordReader
            context.getConfiguration().set(AggregatingRecordReader.START_TOKEN, BEGIN);
            context.getConfiguration().set(AggregatingRecordReader.END_TOKEN, END);
            context.getConfiguration().set(AggregatingRecordReader.RETURN_PARTIAL_MATCHES, Boolean.toString(true));
            // Guard against improper concatenation of newline-separated terms
            context.getConfiguration().set(LONGLINE_NEWLINE_INCLUDED, Boolean.toString(true));

            readerInitializer.initialize(split, context);

            wikiHelper.setup(context.getConfiguration());
        }

        public DataTypeHelper getWikipediaHelper() {
            return wikiHelper;
        }

        public void initializeEvent(Configuration conf) throws IOException {
            Preconditions.checkNotNull(eventInitializer, "eventInitializer cannot be null");
            eventInitializer.initializeEvent(conf);
            try {
                this.parser = FACTORY.newDocumentBuilder();
            } catch (ParserConfigurationException e) {
                throw new IOException("Error instantiating DocumentBuilder", e);
            }
        }

        public RawRecordContainer getEvent(String rawFileName) {
            Preconditions.checkNotNull(currentKeyReader, "currentKeyReader cannot be null");
            Preconditions.checkNotNull(currentValueReader, "currentValueReader cannot be null");
            Preconditions.checkNotNull(event, "event cannot be null");
            Preconditions.checkNotNull(parser, "parser cannot be null");

            event.clear();
            event.setDataType(wikiHelper.getType());
            event.setRawFileName(rawFileName);

            try {
                event.setRawRecordNumber(currentKeyReader.readKey().get());
            } catch (Throwable t) {
                throw new RuntimeException("Unable to get current key", t);
            }

            Text rawPageText = currentValueReader.readValue();

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

            updateEventTypeInformation(event);
            updateEventDate(event);

            if (event instanceof Configurable) {
                event.setId(UID.builder(((Configurable) event).getConf()).newId(data.getBytes(), event.getTimeForUID()));
            } else {
                event.generateId(null);
            }

            if (eventFixer != null) {
                if (event instanceof Configurable) {
                    eventFixer.setup(((Configurable) event).getConf());
                } else {
                    eventFixer.setup(null);
                }
                eventFixer.fixEvent(event);
            }

            return event;
        }

        /**
         * We don't want to call every single wikipedia event "wikipedia" but to use the language and type of the dump to better classify the event
         *
         * @param event
         *            the event container
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
            } catch (DateTimeParseException e) {
                throw new IllegalArgumentException("Could not parse date from filename " + date);
            }
        }
    }
}
