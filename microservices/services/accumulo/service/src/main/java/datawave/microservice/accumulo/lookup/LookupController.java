package datawave.microservice.accumulo.lookup;

import static datawave.microservice.accumulo.lookup.LookupService.ALLOWED_ENCODING;
import static datawave.microservice.accumulo.lookup.LookupService.Parameter.BEGIN_ENTRY;
import static datawave.microservice.accumulo.lookup.LookupService.Parameter.CF;
import static datawave.microservice.accumulo.lookup.LookupService.Parameter.CF_ENCODING;
import static datawave.microservice.accumulo.lookup.LookupService.Parameter.CQ;
import static datawave.microservice.accumulo.lookup.LookupService.Parameter.CQ_ENCODING;
import static datawave.microservice.accumulo.lookup.LookupService.Parameter.END_ENTRY;
import static datawave.microservice.accumulo.lookup.LookupService.Parameter.ROW_ENCODING;
import static datawave.microservice.accumulo.lookup.LookupService.Parameter.USE_AUTHS;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.MediaType;
import org.springframework.security.access.annotation.Secured;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.response.LookupResponse;
import io.swagger.v3.oas.annotations.ExternalDocumentation;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.Parameters;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.tags.Tag;

/**
 * REST controller for Accumulo lookup service
 */
@Tag(name = "Lookup Controller /v1", description = "DataWave Lookup Operations",
                externalDocs = @ExternalDocumentation(description = "Accumulo Service Documentation",
                                url = "https://github.com/NationalSecurityAgency/datawave-accumulo-service"))
@RestController
@ConditionalOnProperty(name = "accumulo.lookup.enabled", havingValue = "true", matchIfMissing = true)
@Secured({"InternalUser", "Administrator"})
@RequestMapping(path = "/v1", produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
public class LookupController {
    
    private final LookupService lookupService;
    
    @Autowired
    public LookupController(LookupService lookupService) {
        this.lookupService = lookupService;
    }
    
    //@formatter:off
    @Operation(summary = "Performs an Accumulo table scan using the given parameters")
    @Parameters({
        @Parameter(name = CF, schema = @Schema(implementation = String.class)),
        @Parameter(name = CQ, schema = @Schema(implementation = String.class)),
        @Parameter(name = BEGIN_ENTRY, schema = @Schema(implementation = Integer.class)),
        @Parameter(name = END_ENTRY, schema = @Schema(implementation = Integer.class)),
        @Parameter(name = USE_AUTHS, schema = @Schema(implementation = String.class, example = "A,B,C,D")),
        @Parameter(name = ROW_ENCODING, schema = @Schema(allowableValues = ALLOWED_ENCODING, implementation = String.class)),
        @Parameter(name = CF_ENCODING, schema = @Schema(allowableValues = ALLOWED_ENCODING, implementation = String.class)),
        @Parameter(name = CQ_ENCODING, schema = @Schema(allowableValues = ALLOWED_ENCODING, implementation = String.class))})
    @RequestMapping(path = "/lookup/{table}/{row}", method = {RequestMethod.GET, RequestMethod.POST})
    public LookupResponse lookup(
        @Parameter(description = "The Accumulo table to be scanned") @PathVariable String table,
        @Parameter(description = "Targeted row within the given table") @PathVariable String row,
        @RequestParam MultiValueMap<String,String> queryParameters,
        @AuthenticationPrincipal DatawaveUserDetails currentUser) throws QueryException {

        LookupService.LookupRequest request = new LookupService.LookupRequest.Builder()
            .withTable(table)
            .withRow(row)
            .withRowEnc(queryParameters.getFirst(ROW_ENCODING))
            .withColFam(queryParameters.getFirst(CF))
            .withColFamEnc(queryParameters.getFirst(CF_ENCODING))
            .withColQual(queryParameters.getFirst(CQ))
            .withColQualEnc(queryParameters.getFirst(CQ_ENCODING))
            .withBeginEntry(queryParameters.getFirst(BEGIN_ENTRY))
            .withEndEntry(queryParameters.getFirst(END_ENTRY))
            .withAuths(queryParameters.getFirst(USE_AUTHS))
            .withParameters(queryParameters)
            .build();

        return lookupService.lookup(request, currentUser);
    }
    //@formatter:on
}
