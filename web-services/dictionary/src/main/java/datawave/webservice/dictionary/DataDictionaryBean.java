package datawave.webservice.dictionary;

import datawave.webservice.datadictionary.RemoteDataDictionary;
import org.apache.http.client.utils.URIBuilder;
import org.xbill.DNS.TextParseException;

import javax.annotation.security.PermitAll;
import javax.ejb.LocalBean;
import javax.ejb.Stateless;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.net.URISyntaxException;

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
        URIBuilder builder = remoteDataDictionary.buildURI(suffix);
        uriInfo.getQueryParameters().forEach((pname, valueList) -> valueList.forEach(pvalue -> builder.addParameter(pname, pvalue)));
        return Response.temporaryRedirect(builder.build()).build();
    }
}
