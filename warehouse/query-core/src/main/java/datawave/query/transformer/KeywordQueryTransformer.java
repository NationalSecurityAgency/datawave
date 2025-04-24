package datawave.query.transformer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;

import datawave.core.query.logic.BaseQueryLogicTransformer;
import datawave.marking.MarkingFunctions;
import datawave.marking.MarkingFunctions.Exception;
import datawave.microservice.query.Query;
import datawave.query.table.parser.KeywordKeyValueFactory;
import datawave.query.tables.keyword.KeywordQueryLogic;
import datawave.query.util.keyword.KeywordResults;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.FieldBase;
import datawave.webservice.query.result.event.Metadata;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.EventQueryResponseBase;

@SuppressWarnings("rawtypes")
public class KeywordQueryTransformer extends BaseQueryLogicTransformer<Entry<Key,Value>,EventBase> {

    private static final Logger log = Logger.getLogger(KeywordQueryTransformer.class);

    protected final Authorizations auths;
    protected final ResponseObjectFactory responseObjectFactory;
    protected final Map<Metadata,String> identifierMap = new HashMap<>();
    protected final Map<Metadata,String> languageMap = new HashMap<>();

    public KeywordQueryTransformer(Query query, MarkingFunctions markingFunctions, ResponseObjectFactory responseObjectFactory) {
        this(query, markingFunctions, responseObjectFactory, false);
    }

    public KeywordQueryTransformer(Query query, MarkingFunctions markingFunctions, ResponseObjectFactory responseObjectFactory, boolean decodeView) {
        super(markingFunctions);
        this.auths = new Authorizations(query.getQueryAuthorizations().split(","));
        this.responseObjectFactory = responseObjectFactory;

        // these maps allow us to recover document identifiers and languages from shard/datatype/uid as long as they are provided in the query.
        extractIdentifiersAndLanguages(query, identifierMap, languageMap);
    }

    /**
     * Extract optional identifiers and languages from query terms. Expected query format is:
     * <p>
     * 'DOCUMENT:shard/datatype/uid!optionalIdentifier1 DOCUMENT:shard/datatype/uid!optionalIdentifier2 ... DOCUMENT:shard/datatype/uid!optionalIdentifier3'
     * <p>
     * The identifiers are not required, so this will parse 'DOCUMENT:shard/datatype/uid' as well.
     * <p>
     * This also supports language tags in the form: 'DOCUMENT:shard/datatype/uid!optionalIdentifier1%LANGUAGE:ENGLISH'
     * </p>
     *
     * @param querySettings
     *            the current query for which we are transforming results.
     * @param identifierMap
     *            used to store mappings between document UIDs and identifiers
     * @param languageMap
     *            used to store mappings between document UIDs and languages
     */
    public static void extractIdentifiersAndLanguages(Query querySettings, Map<Metadata,String> identifierMap, Map<Metadata,String> languageMap) {
        final Collection<String> terms = KeywordQueryLogic.extractQueryTerms(querySettings);
        for (String term : terms) {
            // trim off the field if there is one and discard
            final int fieldSeparation = term.indexOf(':');
            final String valueIdentifierLanguage = fieldSeparation > 0 ? term.substring(fieldSeparation + 1) : term;

            // trim off the language if there is one and preserve it.
            final int languageSeparation = valueIdentifierLanguage.indexOf(KeywordQueryLogic.LANGUAGE_TOKEN);
            final String valueIdentifier = languageSeparation > 0 ? valueIdentifierLanguage.substring(0, languageSeparation) : valueIdentifierLanguage;
            final String language = languageSeparation > 0 ? valueIdentifierLanguage.substring(languageSeparation + KeywordQueryLogic.LANGUAGE_TOKEN.length())
                            : null;

            // trim off the identifier if there is one and preserve it.
            final int identifierSeparation = valueIdentifier.indexOf("!");
            final String value = identifierSeparation > 0 ? valueIdentifier.substring(0, identifierSeparation) : valueIdentifier;
            final String identifier = identifierSeparation > 0 ? valueIdentifier.substring(identifierSeparation + 1) : null;

            // extract shard/dt/uid from the value and store it in a metadata object.
            String[] parts = KeywordQueryLogic.extractUIDParts(value);
            Metadata md = new Metadata();
            md.setRow(parts[0]);
            md.setDataType(parts[1]);
            md.setInternalId(parts[2]);

            if (language != null) {
                languageMap.put(md, language);
                log.debug("Added language " + language + "for pieces: " + parts[0] + ", " + parts[1] + ", " + parts[2]);
            }

            if (identifier != null) {
                identifierMap.put(md, identifier);
                log.debug("Added identifier " + identifier + "for pieces: " + parts[0] + ", " + parts[1] + ", " + parts[2]);
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public EventBase transform(Entry<Key,Value> entry) {

        if (entry.getKey() == null && entry.getValue() == null)
            return null;

        if (null == entry.getKey() || null == entry.getValue()) {
            throw new IllegalArgumentException("Null key or value. Key:" + entry.getKey() + ", Value: " + entry.getValue());
        }

        KeywordKeyValueFactory.KeywordKeyValue kkv;
        try {
            kkv = KeywordKeyValueFactory.parse(entry.getKey(), entry.getValue(), auths, markingFunctions);
        } catch (Exception e1) {
            throw new IllegalArgumentException("Unable to parse visibility", e1);
        }

        EventBase e = responseObjectFactory.getEvent();
        FieldBase field = responseObjectFactory.getField();

        e.setMarkings(kkv.getMarkings());

        // capture the metadata that identifies the field.
        Metadata m = new Metadata();
        m.setRow(kkv.getShardId());
        m.setDataType(kkv.getDatatype());
        m.setInternalId(kkv.getUid());
        e.setMetadata(m);

        // store the content in a field based on its view name.
        field.setMarkings(kkv.getMarkings());
        field.setName(kkv.getViewName() + "_KEYWORDS");
        field.setTimestamp(entry.getKey().getTimestamp());

        try {
            KeywordResults results = KeywordResults.deserialize(kkv.getContents());
            field.setValue(results.get().toString());
        } catch (IOException ioe) {
            field.setValue("Unable to Deserialize");
        }

        List<FieldBase> fields = new ArrayList<>();
        fields.add(field);

        // if an identifier is present for this event, enrich the event with the identifier by adding it as a field.
        String identifier = identifierMap.get(m);
        if (identifier != null) {
            FieldBase idField = responseObjectFactory.getField();
            idField.setMarkings(kkv.getMarkings());
            idField.setName("IDENTIFIER");
            idField.setTimestamp(entry.getKey().getTimestamp());
            idField.setValue(identifier);
            fields.add(idField);
        }

        String language = languageMap.get(m);
        if (language != null) {
            FieldBase idField = responseObjectFactory.getField();
            idField.setMarkings(kkv.getMarkings());
            idField.setName("LANGUAGE");
            idField.setTimestamp(entry.getKey().getTimestamp());
            idField.setValue(language);
            fields.add(idField);
        }

        e.setSizeInBytes(fields.size() * 6);
        e.setFields(fields);

        return e;
    }

    @Override
    public BaseQueryResponse createResponse(List<Object> resultList) {
        EventQueryResponseBase response = responseObjectFactory.getEventQueryResponse();
        List<EventBase> eventList = new ArrayList<>();
        for (Object o : resultList) {
            EventBase result = (EventBase) o;
            eventList.add(result);
        }
        response.setEvents(eventList);
        response.setReturnedEvents((long) eventList.size());
        return response;
    }
}
