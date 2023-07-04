package datawave.ingest.wikipedia;

import java.io.Reader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

/**
 *
 */
public class WikipediaPageExtractor {

    public static final ThreadLocal<SimpleDateFormat> TIMESTAMP_DATE_FORMAT = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'Z"));

    // Extract expected metadata elements about a page
    public static final String TITLE_ELEMENT = "title";
    public static final String ID_ELEMENT = "id";
    public static final String REVISION_ELEMENT = "revision";
    public static final String TIMESTAMP_ELEMENT = "timestamp";
    public static final String CONTRIBUTOR_USERNAME_ELEMENT = "username";
    public static final String CONTRIBUTOR_ID_ELEMENT = "id";
    public static final String MINOR_ELEMENT = "minor";
    public static final String COMMENT_ELEMENT = "comment";
    public static final String TEXT_ELEMENT = "text";
    public static final String SHA1_ELEMENT = "sha1";
    public static final String MODEL_ELEMENT = "model";
    public static final String FORMAT_ELEMENT = "format";

    private static XMLInputFactory xmlif = XMLInputFactory.newInstance();

    static {
        xmlif.setProperty(XMLInputFactory.IS_REPLACING_ENTITY_REFERENCES, Boolean.TRUE);
    }

    public WikipediaPage extract(Reader reader) {

        XMLStreamReader xmlr = null;

        try {
            xmlr = xmlif.createXMLStreamReader(reader);
        } catch (XMLStreamException e1) {
            throw new RuntimeException(e1);
        }

        QName titleName = QName.valueOf("title");
        QName textName = QName.valueOf("text");
        QName revisionName = QName.valueOf("revision");
        QName timestampName = QName.valueOf("timestamp");
        QName commentName = QName.valueOf("comment");
        QName idName = QName.valueOf("id");

        Map<QName,StringBuilder> tags = new HashMap<>();
        for (QName tag : new QName[] {titleName, textName, timestampName, commentName, idName}) {
            tags.put(tag, new StringBuilder());
        }

        StringBuilder articleText = tags.get(textName);
        StringBuilder titleText = tags.get(titleName);
        StringBuilder timestampText = tags.get(timestampName);
        StringBuilder commentText = tags.get(commentName);
        StringBuilder idText = tags.get(idName);

        StringBuilder current = null;
        boolean inRevision = false;
        while (true) {
            try {
                if (!xmlr.hasNext())
                    break;
                xmlr.next();
            } catch (XMLStreamException e) {
                throw new RuntimeException(e);
            }
            QName currentName = null;
            if (xmlr.hasName()) {
                currentName = xmlr.getName();
            }
            if (xmlr.isStartElement() && tags.containsKey(currentName)) {
                if (!inRevision || (!currentName.equals(revisionName) && !currentName.equals(idName))) {
                    current = tags.get(currentName);
                    current.setLength(0);
                }
            } else if (xmlr.isStartElement() && currentName.equals(revisionName)) {
                inRevision = true;
            } else if (xmlr.isEndElement() && currentName.equals(revisionName)) {
                inRevision = false;
            } else if (xmlr.isEndElement() && current != null) {
                if (textName.equals(currentName)) {

                    String title = titleText.toString();
                    String text = articleText.toString();
                    String comment = commentText.toString();
                    int id = Integer.parseInt(idText.toString());
                    long timestamp;
                    try {
                        timestamp = TIMESTAMP_DATE_FORMAT.get().parse(timestampText.append("+0000").toString()).getTime();
                        return new WikipediaPage(id, title, timestamp, comment, text);
                    } catch (ParseException e) {
                        return null;
                    }
                }
                current = null;
            } else if (current != null && xmlr.hasText()) {
                current.append(xmlr.getText());
            }
        }
        return null;
    }
}
