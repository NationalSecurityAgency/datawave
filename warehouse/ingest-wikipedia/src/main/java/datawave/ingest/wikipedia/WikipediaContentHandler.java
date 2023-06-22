package datawave.ingest.wikipedia;

import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 *
 */
public class WikipediaContentHandler implements ContentHandler {
    private final Logger log = Logger.getLogger(WikipediaContentHandler.class);

    private final Set<String> DEPTHS = Sets.newHashSet("page", "revision", "contributor");

    private Multimap<String,String> fields;
    private StringBuilder v = new StringBuilder();
    private Set<String> ignoreFields;

    private Depth currentDepth = Depth.page;

    private enum Depth {
        page, revision, contributor;

        private static final List<Depth> mapping = Lists.newArrayList(Depth.page, Depth.revision, Depth.contributor);

        public static Depth fromOrdinal(int value) {
            if (value < 0 || value >= mapping.size()) {
                throw new IllegalArgumentException("Value out of expected bounds: " + value);
            }

            return mapping.get(value);
        }
    }

    public WikipediaContentHandler(Multimap<String,String> fields, Set<String> ignoreFields) {
        this.fields = fields;
        this.ignoreFields = ignoreFields;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.xml.sax.ContentHandler#setDocumentLocator(org.xml.sax.Locator)
     */
    @Override
    public void setDocumentLocator(Locator locator) {}

    /*
     * (non-Javadoc)
     *
     * @see org.xml.sax.ContentHandler#startDocument()
     */
    @Override
    public void startDocument() throws SAXException {}

    /*
     * (non-Javadoc)
     *
     * @see org.xml.sax.ContentHandler#endDocument()
     */
    @Override
    public void endDocument() throws SAXException {}

    /*
     * (non-Javadoc)
     *
     * @see org.xml.sax.ContentHandler#startPrefixMapping(java.lang.String, java.lang.String)
     */
    @Override
    public void startPrefixMapping(String prefix, String uri) throws SAXException {}

    /*
     * (non-Javadoc)
     *
     * @see org.xml.sax.ContentHandler#endPrefixMapping(java.lang.String)
     */
    @Override
    public void endPrefixMapping(String prefix) throws SAXException {}

    /*
     * (non-Javadoc)
     *
     * @see org.xml.sax.ContentHandler#startElement(java.lang.String, java.lang.String, java.lang.String, org.xml.sax.Attributes)
     */
    @Override
    public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
        updateDepthEntry(localName);

        // Take each attribute specified on this element and create an index entry for it
        for (int i = 0; i < atts.getLength(); i++) {
            String value = atts.getValue(i);

            if (!StringUtils.isBlank(value)) {
                String fieldName = getFieldName(localName + "_" + atts.getLocalName(i));

                if (!ignoreFields.contains(fieldName)) {
                    if (WikipediaIngestHelper.fieldNameReplacements.containsKey(fieldName)) {
                        fieldName = WikipediaIngestHelper.fieldNameReplacements.get(fieldName);
                    }

                    fields.put(fieldName, StringUtils.trim(value));
                }
            }
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.xml.sax.ContentHandler#endElement(java.lang.String, java.lang.String, java.lang.String)
     */
    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        if (v.length() == 0) {
            log.debug("no value for " + localName);
            updateDepthExit(localName);
            return;
        }

        String fieldName = getFieldName(localName);

        if (!ignoreFields.contains(fieldName)) {
            String value = v.toString().trim();

            if (!value.isEmpty()) {
                if (WikipediaIngestHelper.fieldNameReplacements.containsKey(fieldName)) {
                    fieldName = WikipediaIngestHelper.fieldNameReplacements.get(fieldName);
                }

                fields.put(fieldName, value);
            }
        }

        v.delete(0, v.length());

        updateDepthExit(localName);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.xml.sax.ContentHandler#characters(char[], int, int)
     */
    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        if (log.isTraceEnabled()) {
            log.trace("Text: start:" + start + ", length: " + length + "value: " + new String(ch, start, length));
        }

        v.append(ch, start, length);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.xml.sax.ContentHandler#ignorableWhitespace(char[], int, int)
     */
    @Override
    public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {}

    /*
     * (non-Javadoc)
     *
     * @see org.xml.sax.ContentHandler#processingInstruction(java.lang.String, java.lang.String)
     */
    @Override
    public void processingInstruction(String target, String data) throws SAXException {}

    /*
     * (non-Javadoc)
     *
     * @see org.xml.sax.ContentHandler#skippedEntity(java.lang.String)
     */
    @Override
    public void skippedEntity(String name) throws SAXException {}

    protected void updateDepthEntry(String localName) {
        // If we can't figure out what this is, I don't care.
        if (DEPTHS.contains(localName)) {
            this.currentDepth = Depth.valueOf(localName);
        }
    }

    protected void updateDepthExit(String localName) {
        // If we can't figure out what this is, I don't care.
        if (DEPTHS.contains(localName)) {
            int ord = Depth.valueOf(localName).ordinal();

            // Wind back up the hierarchy
            if (ord > 1) {
                this.currentDepth = Depth.fromOrdinal(ord - 1);
            }
        }
    }

    protected String getFieldName(String localName) {
        checkNotNull(localName);

        // Put on the prefix for this field to keep the fields labeled uniquely (e.g. ID to REVISION_ID)
        return (this.currentDepth + "_" + localName.trim()).toUpperCase();
    }
}
