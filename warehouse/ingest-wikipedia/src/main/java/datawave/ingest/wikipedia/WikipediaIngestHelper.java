package datawave.ingest.wikipedia;

import java.io.StringReader;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import datawave.ingest.data.RawDataErrorNames;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.ingest.IndexOnlyIngestHelperInterface;
import datawave.ingest.data.config.ingest.TermFrequencyIngestHelperInterface;
import datawave.ingest.mapreduce.handler.tokenize.ExtendedContentIngestHelper;

/**
 *
 */
public class WikipediaIngestHelper extends ExtendedContentIngestHelper implements TermFrequencyIngestHelperInterface, IndexOnlyIngestHelperInterface {
    private static final Logger log = Logger.getLogger(WikipediaIngestHelper.class);

    private static final String LANGUAGE = "LANGUAGE";
    private static final String WIKI = "wiki";

    protected WikipediaHelper helper = new WikipediaHelper();
    protected HashSet<String> ignoreFields = Sets.newHashSet("REVISION_TEXT");

    public static final Map<String,String> fieldNameReplacements = ImmutableMap.<String,String> builder().put("PAGE_NS", "PAGE_NAMESPACE").build();

    public WikipediaHelper getDataTypeHelper() {
        return this.helper;
    }

    @Override
    public void setup(Configuration config) throws IllegalArgumentException {
        super.setup(config);
        helper.setup(config);
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.ingest.data.config.ingest.AbstractIngestHelper#getEventFields(datawave.ingest.data.Event)
     */
    @Override
    public Multimap<String,NormalizedContentInterface> getEventFields(RawRecordContainer event) {
        HashMultimap<String,String> fields = HashMultimap.create();

        // Get the raw data
        String data = new String(event.getRawData());
        // Wrap the data
        StringReader reader = new StringReader(data);
        InputSource source = new InputSource(reader);

        // Create an XML parser
        try {
            WikipediaContentHandler handler = new WikipediaContentHandler(fields, ignoreFields);
            XMLReader parser = XMLReaderFactory.createXMLReader();
            parser.setFeature("http://apache.org/xml/features/disallow-doctype-decl", false);
            parser.setFeature("http://xml.org/sax/features/external-general-entities", false);
            parser.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
            parser.setContentHandler(handler);
            parser.parse(source);
        } catch (Exception e) {
            // If error, return empty results map.
            log.error("Error processing Wikipedia XML document", e);
            event.addError(RawDataErrorNames.FIELD_EXTRACTION_ERROR);
        }

        extractWikipediaTypeInformation(event, fields);

        return normalize(fields);
    }

    protected void extractWikipediaTypeInformation(RawRecordContainer event, Multimap<String,String> fields) {
        String typeName = event.getDataType().outputName();

        int index = typeName.indexOf(WIKI);
        if (index < 0) {
            return;
        }

        String dumpName = typeName.substring(0, index);
        String languageCode = dumpName;
        index = dumpName.indexOf('_');

        if (0 <= index) {
            languageCode = dumpName.substring(0, index);
        }

        // See if we can guess at the language given the prefix on the datatype
        if (ISO_639_Codes.ISO_639_1.containsKey(languageCode)) {
            Collection<String> languages = ISO_639_Codes.ISO_639_1.get(languageCode);
            fields.putAll(LANGUAGE, languages);
        }
    }

    public Set<NormalizedContentInterface> normalize(NormalizedContentInterface field) {
        return super.normalize(field);
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.ingest.data.config.ingest.IndexOnlyIngestHelperInterface#getIndexOnlyFields()
     */
    @Override
    public Set<String> getIndexOnlyFields() {
        return this.indexOnlyFields;
    }

    @Override
    public boolean isTermFrequencyField(String fieldName) {
        return helper.getContentIndexedFields().contains(fieldName);
    }
}
