package datawave.webservice.dictionary;

import java.net.URISyntaxException;

import jakarta.annotation.security.PermitAll;
import jakarta.ejb.LocalBean;
import jakarta.ejb.Stateless;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;

import org.apache.http.client.utils.URIBuilder;
import org.xbill.DNS.TextParseException;

import datawave.webservice.datadictionary.RemoteDataDictionary;

/**
 * A simple proxy that redirects GET requests for the DataDictionary to the external dictionary service that is configured in the {@link RemoteDataDictionary}.
 * This allows existing documentation URLs to continue to work.
 */
@Path("/DataDictionary")
@LocalBean
@Stateless
@PermitAll
public class DataDictionaryBean {

    @Inject
    private RemoteDataDictionary remoteDataDictionary;

    /*
     * Capture and redirect GET requests to the root path for the data dictionary.
     */
    @GET
    @Path("/")
    public Response getDataDictionary(@Context UriInfo uriInfo) throws TextParseException, URISyntaxException {
        return sendRedirect("", uriInfo);
    }

    /*
     * Capture and redirect GET requests to any sub-path for the data dictionary.
     */
    @GET
    @Path("/{suffix : .*}")
    public Response getDataDictionaryWithSuffix(@PathParam("suffix") String suffix, @Context UriInfo uriInfo) throws TextParseException, URISyntaxException {
        return sendRedirect(suffix, uriInfo);
    }

    private Response sendRedirect(String suffix, UriInfo uriInfo) throws TextParseException, URISyntaxException {
        URIBuilder builder = remoteDataDictionary.buildRedirectURI(suffix, uriInfo.getBaseUri());
        uriInfo.getQueryParameters().forEach((pname, valueList) -> valueList.forEach(pvalue -> builder.addParameter(pname, pvalue)));
        return Response.temporaryRedirect(builder.build()).build();
    }
}
