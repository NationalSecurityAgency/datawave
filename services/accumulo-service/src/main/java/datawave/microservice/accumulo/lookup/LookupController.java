package datawave.microservice.accumulo.lookup;

import datawave.microservice.accumulo.lookup.LookupService.Parameter;
import datawave.microservice.authorization.user.ProxiedUserDetails;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.response.LookupResponse;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.MediaType;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.security.RolesAllowed;

import static datawave.microservice.accumulo.lookup.LookupService.ALLOWED_ENCODING;

/**
 * REST controller for Accumulo lookup service
 */
@RestController
@ConditionalOnProperty(name = "accumulo.lookup.enabled", havingValue = "true", matchIfMissing = true)
@RolesAllowed({"InternalUser", "Administrator"})
@RequestMapping(path = "/v1", produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
public class LookupController {
    
    private final LookupService lookupService;
    
    @Autowired
    public LookupController(LookupService lookupService) {
        this.lookupService = lookupService;
    }
    
    //@formatter:off
    @ApiOperation(value = "Performs an Accumulo table scan using the given parameters")
    @ApiImplicitParams({
        @ApiImplicitParam(name = Parameter.CF, dataTypeClass = String.class),
        @ApiImplicitParam(name = Parameter.CQ, dataTypeClass = String.class),
        @ApiImplicitParam(name = Parameter.BEGIN_ENTRY, dataTypeClass = Integer.class),
        @ApiImplicitParam(name = Parameter.END_ENTRY, dataTypeClass = Integer.class),
        @ApiImplicitParam(name = Parameter.USE_AUTHS, dataTypeClass = String.class, example = "A,B,C,D"),
        @ApiImplicitParam(name = Parameter.ROW_ENCODING, allowableValues = ALLOWED_ENCODING, dataTypeClass = String.class),
        @ApiImplicitParam(name = Parameter.CF_ENCODING, allowableValues = ALLOWED_ENCODING, dataTypeClass = String.class),
        @ApiImplicitParam(name = Parameter.CQ_ENCODING, allowableValues = ALLOWED_ENCODING, dataTypeClass = String.class)})
    @RequestMapping(path = "/lookup/{table}/{row}", method = {RequestMethod.GET, RequestMethod.POST})
    public LookupResponse lookup(
        @ApiParam("The Accumulo table to be scanned") @PathVariable String table,
        @ApiParam("Targeted row within the given table") @PathVariable String row,
        @RequestParam MultiValueMap<String,String> queryParameters,
        @AuthenticationPrincipal ProxiedUserDetails currentUser) throws QueryException {

        LookupService.LookupRequest request = new LookupService.LookupRequest.Builder()
            .withTable(table)
            .withRow(row)
            .withRowEnc(queryParameters.getFirst(Parameter.ROW_ENCODING))
            .withColFam(queryParameters.getFirst(Parameter.CF))
            .withColFamEnc(queryParameters.getFirst(Parameter.CF_ENCODING))
            .withColQual(queryParameters.getFirst(Parameter.CQ))
            .withColQualEnc(queryParameters.getFirst(Parameter.CQ_ENCODING))
            .withBeginEntry(queryParameters.getFirst(Parameter.BEGIN_ENTRY))
            .withEndEntry(queryParameters.getFirst(Parameter.END_ENTRY))
            .withAuths(queryParameters.getFirst(Parameter.USE_AUTHS))
            .withParameters(queryParameters)
            .build();

        return lookupService.lookup(request, currentUser);
    }
    //@formatter:on
}
