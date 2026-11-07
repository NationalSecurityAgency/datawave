package datawave.webservice.dictionary;

import java.net.URISyntaxException;

import jakarta.annotation.security.PermitAll;
import jakarta.ejb.LocalBean;
import jakarta.ejb.Stateless;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;

import org.apache.http.client.utils.URIBuilder;
import org.xbill.DNS.TextParseException;

import datawave.webservice.edgedictionary.RemoteEdgeDictionary;

/**
 * A simple proxy that redirects GET requests for the EdgeDictionary to the external dictionary service that is configured in the {@link RemoteEdgeDictionary}.
 * This allows existing documentation URLs to continue to work.
 */
@Path("/EdgeDictionary")
@LocalBean
@Stateless
@PermitAll
public class EdgeDictionaryBean {

    @Inject
    private RemoteEdgeDictionary remoteEdgeDictionary;

    /**
     * @param uriInfo
     *            uriInfo The edge dictionary only has one endpoint. Send redirects for any request to it.
     * @throws TextParseException
     *             for issues with parsing
     * @throws URISyntaxException
     *             for syntax issues
     * @return a redirect response
     */
    @GET
    @Path("/")
    public Response getEdgeDictionary(@Context UriInfo uriInfo) throws TextParseException, URISyntaxException {
        URIBuilder builder = remoteEdgeDictionary.buildRedirectURI("", uriInfo.getBaseUri());
        uriInfo.getQueryParameters().forEach((pname, valueList) -> valueList.forEach(pvalue -> builder.addParameter(pname, pvalue)));
        return Response.temporaryRedirect(builder.build()).build();
    }
}
