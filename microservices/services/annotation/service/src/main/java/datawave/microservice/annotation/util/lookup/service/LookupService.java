package datawave.microservice.annotation.util.lookup.service;

import static datawave.microservice.annotation.util.AuthUtils.buildOutgoingHeaders;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import datawave.microservice.annotation.util.Metadata;
import datawave.microservice.annotation.util.exceptions.ResourceNotFoundException;
import datawave.microservice.annotation.util.lookup.common.Lookup;
import datawave.microservice.annotation.util.lookup.common.LookupRequest;
import datawave.microservice.annotation.util.lookup.common.ParsedResponse;
import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.result.BaseResponse;
import datawave.webservice.result.EventQueryResponseBase;
import lombok.extern.slf4j.Slf4j;

/** Used to perform lookupUUID request against a remote instance of Datawave */
@Slf4j
@Service
public class LookupService {

    private final LookupRequest request;

    /**
     *
     * @param request
     *            the configuration
     */
    @Autowired
    public LookupService(LookupRequest request) {
        this.request = request;
    }

    /**
     * Executes a lookupUUID query for the specified idType and it and returns the item's internal identifiers (e.g., shard, datatype, uid) packaged in a
     * Metadata object.
     *
     * @param idType
     *            the type of id to query.
     * @param id
     *            the id value to query.
     * @param queryParams
     *            the encoded query parameters to send with the remote lookup request
     * @param systemFrom
     *            the systemFrom to indicate in the query params
     * @param currentUser
     *            the current user whose authorization headers should be forwarded to the remote lookup
     * @return a list of zero to many Metadata objects with the internal shard, datatype, uid and table name of the identifier(s) provided. The list will be
     *         empty if no identifier could be found using the authorizations and query logic employed by this class.
     * @throws ResourceNotFoundException
     *             if exceptions are encountered performing the lookup.
     */
    public List<Metadata> executeLookupUUIDQuery(String idType, String id, String queryParams, String systemFrom, DatawaveUserDetails currentUser) {
        Map<String,String> headers = buildOutgoingHeaders(currentUser, null);
        ParsedResponse parsedResponse = request.lookupId(Lookup.METADATA, idType + "/" + id, headers, queryParams, systemFrom);
        BaseResponse response = request.parseQueryResponse(parsedResponse.getResponse());
        if (!(EventQueryResponseBase.class.isAssignableFrom(response.getClass()))) {
            throw new ResourceNotFoundException("Unexpected Response Class received from Datawave: " + response.getClass().getSimpleName());
        }
        EventQueryResponseBase eventQueryResponseBase = (EventQueryResponseBase) response;
        @SuppressWarnings("rawtypes")
        Iterator<EventBase> iter = eventQueryResponseBase.getEvents().iterator();
        List<Metadata> metadataList = new ArrayList<>();
        while (iter.hasNext()) {
            EventBase<?,?> e = iter.next();
            datawave.webservice.query.result.event.Metadata eventMetadata = e.getMetadata();
            if (log.isDebugEnabled()) {

                String metadataMessage = String.format("%s/%s/%s [%s]", eventMetadata.getRow(), eventMetadata.getDataType(), eventMetadata.getInternalId(),
                                eventMetadata.getTable());
                log.debug("Found metadata {} for idType {}, id {}", metadataMessage, idType, id);
            }
            metadataList.add(new Metadata(eventMetadata));
        }
        return metadataList;
    }
}
