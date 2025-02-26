package datawave.microservice.query.mapreduce;

import static datawave.microservice.query.QueryParameters.QUERY_AUTHORIZATIONS;
import static datawave.microservice.query.mapreduce.config.MapReduceQueryProperties.FORMAT;
import static datawave.microservice.query.mapreduce.config.MapReduceQueryProperties.JOB_NAME;
import static datawave.microservice.query.mapreduce.config.MapReduceQueryProperties.OUTPUT_FORMAT;
import static datawave.microservice.query.mapreduce.config.MapReduceQueryProperties.OUTPUT_TABLE_NAME;
import static datawave.microservice.query.mapreduce.config.MapReduceQueryProperties.PARAMETERS;
import static datawave.microservice.query.mapreduce.config.MapReduceQueryProperties.QUERY_ID;
import static datawave.microservice.query.mapreduce.jobs.OozieJob.WORKFLOW;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.annotation.Secured;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.microservice.query.mapreduce.config.MapReduceQueryProperties;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.result.GenericResponse;
import datawave.webservice.result.VoidResponse;
import datawave.webservice.results.mr.MapReduceInfoResponseList;
import datawave.webservice.results.mr.MapReduceJobDescriptionList;
import io.swagger.v3.oas.annotations.ExternalDocumentation;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.Parameters;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "MapReduce Query Controller /v1", description = "DataWave MapReduce Query Management",
                externalDocs = @ExternalDocumentation(description = "MapReduce Query Service Documentation",
                                url = "https://github.com/NationalSecurityAgency/datawave-mapreduce-query-service"))
@RestController
@RequestMapping(path = "/v1/mapreduce", produces = MediaType.APPLICATION_JSON_VALUE)
@ConditionalOnProperty(name = MapReduceQueryProperties.PREFIX + ".enabled", havingValue = "true", matchIfMissing = true)
public class MapReduceQueryController {
    private final MapReduceQueryManagementService mapReduceQueryManagementService;
    
    public MapReduceQueryController(MapReduceQueryManagementService mapReduceQueryManagementService) {
        this.mapReduceQueryManagementService = mapReduceQueryManagementService;
    }
    
    // @see MapReduceQueryManagementService#listConfigurations(String, DatawaveUserDetails)
    // @formatter:off
    @Operation(
            summary = "Gets a list of the available map reduce jobs and their configurations.",
            description = "Returns all matching map reduce jobs available to the user, filtering by job type.")
    @ApiResponses({
            @ApiResponse(
                    description = "if successful, returns a map reduce list response containing the matching job configurations",
                    responseCode = "200",
                    content = @Content(schema = @Schema(implementation = MapReduceJobDescriptionList.class))),
            @ApiResponse(
                    description = "if there is an unknown error",
                    responseCode = "500",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class)))})
    // @formatter:on
    @RequestMapping(path = "listConfigurations", method = RequestMethod.GET, produces = {"application/xml", "text/xml", "application/json", "text/yaml",
            "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public MapReduceJobDescriptionList listConfigurations(
                    @Parameter(description = "The type of jobs to list") @RequestParam(required = false, defaultValue = "none") String jobType,
                    @AuthenticationPrincipal DatawaveUserDetails currentUser) {
        return mapReduceQueryManagementService.listConfigurations(jobType, currentUser);
    }
    
    // @see MapReduceQueryManagementService#oozieSubmit(MultiValueMap, DatawaveUserDetails)
    // @formatter:off
    @Operation(
            summary = "Execute a configured oozie workflow.",
            description = "Runs the selected oozie workflow.")
    @ApiResponses({
            @ApiResponse(
                    description = "if successful, returns a generic response containing the oozie workflow id",
                    responseCode = "200",
                    content = @Content(schema = @Schema(implementation = GenericResponse.class))),
            @ApiResponse(
                    description = "if the job configuration can't be found<br>" +
                            "if parameter validation fails<br>" +
                            "if auditing fails",
                    responseCode = "400",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if the user doesn't have access to the requested job configuration",
                    responseCode = "401",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if query storage fails<br>" +
                            "if there is an unknown error",
                    responseCode = "500",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class)))})
    @Parameters({
            @Parameter(
                    name = WORKFLOW,
                    in = ParameterIn.QUERY,
                    description = "The oozie workflow to execute",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "OozieJob"),
            @Parameter(
                    name = QUERY_AUTHORIZATIONS,
                    in = ParameterIn.QUERY,
                    description = "The query auths",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "PUBLIC,PRIVATE,BAR,FOO"),
            @Parameter(
                    name = PARAMETERS,
                    in = ParameterIn.QUERY,
                    description = "Additional query parameters",
                    schema = @Schema(implementation = String.class),
                    example = "KEY_1:VALUE_1;KEY_2:VALUE_2")
    })
    // @formatter:on
    @RequestMapping(path = "oozieSubmit", method = RequestMethod.POST, produces = {"application/xml", "text/xml", "application/json", "text/yaml",
            "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public GenericResponse<String> oozieSubmit(@Parameter(hidden = true) @RequestParam MultiValueMap<String,String> parameters,
                    @AuthenticationPrincipal DatawaveUserDetails currentUser) throws QueryException {
        return mapReduceQueryManagementService.oozieSubmit(parameters, currentUser);
    }
    
    // @see MapReduceQueryManagementService#submit(MultiValueMap, DatawaveUserDetails)
    // @formatter:off
    @Operation(
            summary = "Execute a configured map reduce job.",
            description = "Runs the selected map reduce job.<br>" +
                    "Loads the specified defined query, and runs it as a map reduce job.<br>" +
                    "By default, results will be written to an HDFS output directory.<br>" +
                    "If 'outputTableName' is specified, results will be written to a table in Accumulo instead.")
    @ApiResponses({
            @ApiResponse(
                    description = "if successful, returns a generic response containing the map reduce query id",
                    responseCode = "200",
                    content = @Content(schema = @Schema(implementation = GenericResponse.class))),
            @ApiResponse(
                    description = "if the job configuration can't be found<br>" +
                            "if parameter validation fails<br>" +
                            "if auditing fails",
                    responseCode = "400",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if the user doesn't have access to the requested job configuration<br>" +
                            "if the user doesn't own the defined query",
                    responseCode = "401",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if the query cannot be found",
                    responseCode = "404",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if query storage fails<br>" +
                            "if there is an unknown error",
                    responseCode = "500",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class)))})
    @Parameters({
            @Parameter(
                    name = JOB_NAME,
                    in = ParameterIn.QUERY,
                    description = "The name of the map reduce job configuration to execute",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "OozieJob"),
            @Parameter(
                    name = QUERY_AUTHORIZATIONS,
                    in = ParameterIn.QUERY,
                    description = "The query auths",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "PUBLIC,PRIVATE,BAR,FOO"),
            @Parameter(
                    name = QUERY_ID,
                    in = ParameterIn.QUERY,
                    description = "The id of the query to run as a map reduce job",
                    required = true,
                    schema = @Schema(implementation = String.class)),
            @Parameter(
                    name = FORMAT,
                    in = ParameterIn.QUERY,
                    description = "The serialization format to use when writing results",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "XML"),
            @Parameter(
                    name = OUTPUT_TABLE_NAME,
                    in = ParameterIn.QUERY,
                    description = "The name of the table where the results should be written",
                    schema = @Schema(implementation = String.class)),
            @Parameter(
                    name = OUTPUT_FORMAT,
                    in = ParameterIn.QUERY,
                    description = "The hadoop file output format to use when writing results",
                    schema = @Schema(implementation = String.class),
                    example = "TEXT"),
            @Parameter(
                    name = PARAMETERS,
                    in = ParameterIn.QUERY,
                    description = "Additional query parameters",
                    schema = @Schema(implementation = String.class),
                    example = "KEY_1:VALUE_1;KEY_2:VALUE_2")
    })
    // @formatter:on
    @RequestMapping(path = "submit", method = RequestMethod.POST, produces = {"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml",
            "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public GenericResponse<String> submit(@Parameter(hidden = true) @RequestParam MultiValueMap<String,String> parameters,
                    @AuthenticationPrincipal DatawaveUserDetails currentUser) throws QueryException {
        return mapReduceQueryManagementService.submit(parameters, currentUser);
    }
    
    // @see MapReduceQueryManagementService#cancel(String, DatawaveUserDetails)
    // @formatter:off
    @Operation(
            summary = "Cancels the specified query.",
            description = "Cancel can only be called on a running query.<br>" +
                    "Aside from admins, only the query owner can cancel the specified query.")
    @ApiResponses({
            @ApiResponse(
                    description = "if successful, returns a void response indicating that the query was canceled",
                    responseCode = "200",
                    content = @Content(schema = @Schema(implementation = GenericResponse.class))),
            @ApiResponse(
                    description = "if the query is not running",
                    responseCode = "400",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if the user doesn't own the query",
                    responseCode = "401",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if the query cannot be found",
                    responseCode = "404",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if query lock acquisition fails<br>" +
                            "if the cancel call is interrupted<br>" +
                            "if there is an unknown error",
                    responseCode = "500",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class)))})
    // @formatter:on
    @RequestMapping(path = "{id}/cancel", method = {RequestMethod.POST, RequestMethod.PUT}, produces = {"application/xml", "text/xml", "application/json",
            "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public GenericResponse<Boolean> cancel(@Parameter(description = "The map reduce query id") @PathVariable String id,
                    @AuthenticationPrincipal DatawaveUserDetails currentUser) throws QueryException {
        return mapReduceQueryManagementService.cancel(id, currentUser);
    }
    
    // @see MapReduceQueryManagementService#adminCancel(String, DatawaveUserDetails)
    // @formatter:off
    @Operation(
            summary = "Cancels the specified query using admin privileges.",
            description = "Cancel can only be called on a running query.<br>" +
                    "Only admin users should be allowed to call this method.")
    @ApiResponses({
            @ApiResponse(
                    description = "if successful, returns a void response indicating that the query was canceled",
                    responseCode = "200",
                    content = @Content(schema = @Schema(implementation = GenericResponse.class))),
            @ApiResponse(
                    description = "if the query is not running",
                    responseCode = "400",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if the query cannot be found",
                    responseCode = "404",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if query lock acquisition fails<br>" +
                            "if the cancel call is interrupted<br>" +
                            "if there is an unknown error",
                    responseCode = "500",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class)))})
    // @formatter:on
    @Secured({"Administrator", "JBossAdministrator"})
    @RequestMapping(path = "{id}/adminCancel", method = {RequestMethod.POST, RequestMethod.PUT}, produces = {"application/xml", "text/xml", "application/json",
            "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public GenericResponse<Boolean> adminCancel(@Parameter(description = "The map reduce query id") @PathVariable String id,
                    @AuthenticationPrincipal DatawaveUserDetails currentUser) throws QueryException {
        return mapReduceQueryManagementService.adminCancel(id, currentUser);
    }
    
    // @see MapReduceQueryManagementService#restart(String, DatawaveUserDetails)
    // @formatter:off
    @Operation(
            summary = "Stops, and restarts the specified query.",
            description = "Restart can be called on any query, whether it's running or not.<br>" +
                    "If the specified query is still running, it will be canceled. See <strong>cancel</strong>.<br>" +
                    "Restart creates a new, identical query, with a new query id.<br>" +
                    "Restart queries will start running immediately.<br>" +
                    "Auditing is performed before the new query is started.")
    @ApiResponses({
            @ApiResponse(
                    description = "if successful, returns a generic response containing the new query id",
                    responseCode = "200",
                    content = @Content(schema = @Schema(implementation = GenericResponse.class))),
            @ApiResponse(
                    description = "if parameter validation fails<br>" +
                            "if auditing fails",
                    responseCode = "400",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if the user doesn't have access to the requested job configuration<br>" +
                            "if the user doesn't own the defined query",
                    responseCode = "401",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if the query cannot be found",
                    responseCode = "404",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if query storage fails<br>" +
                            "if there is an unknown error",
                    responseCode = "500",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class)))})
    // @formatter:on
    @RequestMapping(path = "{id}/restart", method = {RequestMethod.PUT, RequestMethod.POST}, produces = {"application/xml", "text/xml", "application/json",
            "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public GenericResponse<String> restart(@Parameter(description = "The map reduce query id") @PathVariable String id,
                    @AuthenticationPrincipal DatawaveUserDetails currentUser) throws QueryException {
        return mapReduceQueryManagementService.restart(id, currentUser);
    }
    
    // @see MapReduceQueryManagementService#list(String, DatawaveUserDetails)
    // @formatter:off
    @Operation(
            summary = "Gets a list of result info for the specified query for the calling user.",
            description = "Returns a list of result info for the specified query owned by the calling user.")
    @ApiResponses({
            @ApiResponse(
                    description = "if successful, returns a map reduce info list response",
                    responseCode = "200",
                    content = @Content(schema = @Schema(implementation = MapReduceInfoResponseList.class))),
            @ApiResponse(
                    description = "if there is an unknown error",
                    responseCode = "500",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class)))})
    // @formatter:on
    @RequestMapping(path = "{id}/list", method = RequestMethod.GET, produces = {"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml",
            "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public MapReduceInfoResponseList list(@Parameter(description = "The map reduce query id") @PathVariable String id,
                    @AuthenticationPrincipal DatawaveUserDetails currentUser) throws QueryException {
        return mapReduceQueryManagementService.list(id, currentUser);
    }
    
    // @see MapReduceQueryManagementService#getFile(String,String, DatawaveUserDetails)
    // @formatter:off
    @Operation(
            summary = "Gets a result file for the specified query for the calling user.",
            description = "Returns a result file for the specified query owned by the calling user.")
    @ApiResponses({
            @ApiResponse(
                    description = "if successful, a map reduce query result file",
                    responseCode = "200",
                    content = @Content(schema = @Schema(implementation = StreamingResponseBody.class))),
            @ApiResponse(
                    description = "if there is an unknown error",
                    responseCode = "500",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class)))})
    // @formatter:on
    @RequestMapping(path = "{id}/getFile/{fileName}", method = RequestMethod.GET, produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public ResponseEntity<StreamingResponseBody> getFile(@Parameter(description = "The map reduce query id") @PathVariable String id,
                    @Parameter(description = "The file name") @PathVariable String fileName, @AuthenticationPrincipal DatawaveUserDetails currentUser)
                    throws QueryException {
        final Map.Entry<FileStatus,FSDataInputStream> resultFile = mapReduceQueryManagementService.getFile(id, fileName, currentUser);
        
        // @formatter:off
        return ResponseEntity.ok()
                .contentType(MediaType.APPLICATION_OCTET_STREAM)
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + fileName + "\"").body(outputStream -> {
                    try (FSDataInputStream inputStream = resultFile.getValue()) {
                        IOUtils.copy(inputStream, outputStream);
                    }
                });
        // @formatter:on
    }
    
    // @see MapReduceQueryManagementService#getAllFiles(String, DatawaveUserDetails)
    // @formatter:off
    @Operation(
            summary = "Gets all result files for the specified query for the calling user.",
            description = "Returns all result files for the specified query owned by the calling user.")
    @ApiResponses({
            @ApiResponse(
                    description = "if successful, a tarball containing map reduce query result files",
                    responseCode = "200",
                    content = @Content(schema = @Schema(implementation = StreamingResponseBody.class))),
            @ApiResponse(
                    description = "if there is an unknown error",
                    responseCode = "500",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class)))})
    // @formatter:on
    @RequestMapping(path = "{id}/getAllFiles", method = RequestMethod.GET, produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public ResponseEntity<StreamingResponseBody> getAllFiles(@Parameter(description = "The map reduce query id") @PathVariable String id,
                    @AuthenticationPrincipal DatawaveUserDetails currentUser) throws QueryException {
        final Map<FileStatus,FSDataInputStream> resultFiles = mapReduceQueryManagementService.getAllFiles(id, currentUser);
        
        // @formatter:off
        return ResponseEntity.ok()
                .contentType(MediaType.APPLICATION_OCTET_STREAM)
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + id + ".tar\"")
                .body(outputStream -> {
                    TarArchiveOutputStream tarArchiveOutputStream = new TarArchiveOutputStream(outputStream);
                    tarArchiveOutputStream.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);
                    try {
                        for (Map.Entry<FileStatus, FSDataInputStream> resultFile : resultFiles.entrySet()) {
                            String fileName = resultFile.getKey().getPath().toString();
                            TarArchiveEntry entry = new TarArchiveEntry(id + "/" + fileName, false);
                            entry.setSize(resultFile.getKey().getLen());
                            tarArchiveOutputStream.putArchiveEntry(entry);
                            try {
                                IOUtils.copy(resultFile.getValue(), tarArchiveOutputStream);
                            } finally {
                                resultFile.getValue().close();
                            }
                            tarArchiveOutputStream.closeArchiveEntry();
                        }
                        tarArchiveOutputStream.finish();
                    } finally {
                        try {
                            tarArchiveOutputStream.close();
                        } catch (IOException ioe) {
                            // do nothing
                        }
                    }
                });
        // @formatter:on
    }
    
    // @see MapReduceQueryManagementService#list(DatawaveUserDetails)
    // @formatter:off
    @Operation(
            summary = "Gets a list of result info for all map reduce queries owned by the calling user.",
            description = "Returns a list of result info for all map reduce queries owned by the calling user.")
    @ApiResponses({
            @ApiResponse(
                    description = "if successful, returns a map reduce info list response",
                    responseCode = "200",
                    content = @Content(schema = @Schema(implementation = MapReduceInfoResponseList.class))),
            @ApiResponse(
                    description = "if there is an unknown error",
                    responseCode = "500",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class)))})
    // @formatter:on
    @RequestMapping(path = "list", method = RequestMethod.GET, produces = {"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml",
            "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public MapReduceInfoResponseList list(@AuthenticationPrincipal DatawaveUserDetails currentUser) throws QueryException {
        return mapReduceQueryManagementService.list(currentUser);
    }
    
    // @see MapReduceQueryManagementService#remove(String, DatawaveUserDetails)
    // @formatter:off
    @Operation(
            summary = "Removes the specified map reduce query from query storage.",
            description = "If the map reduce query is running, it wil be canceled.<br>" +
                    "Aside from admins, only the query owner can remove the specified query.")
    @ApiResponses({
            @ApiResponse(
                    description = "if successful, returns a void response indicating that the map reduce query was removed",
                    responseCode = "200",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if the user doesn't own the query",
                    responseCode = "401",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if the query cannot be found",
                    responseCode = "404",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if there is an unknown error",
                    responseCode = "500",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class)))})
    // @formatter:on
    @RequestMapping(path = "{id}/remove", method = RequestMethod.DELETE, produces = {"application/xml", "text/xml", "application/json", "text/yaml",
            "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public VoidResponse remove(@Parameter(description = "The map reduce query id") @PathVariable String id,
                    @AuthenticationPrincipal DatawaveUserDetails currentUser) throws QueryException {
        return mapReduceQueryManagementService.remove(id, currentUser);
    }
    
    // @see MapReduceQueryManagementService#adminRemove(String, DatawaveUserDetails)
    // @formatter:off
    @Operation(
            summary = "Removes the specified map reduce query from query storage using admin privileges.",
            description = "If the map reduce query is running, it wil be canceled.<br>" +
                    "Only admin users should be allowed to call this method.")
    @ApiResponses({
            @ApiResponse(
                    description = "if successful, returns a void response indicating that the map reduce query was removed",
                    responseCode = "200",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if the user doesn't own the query",
                    responseCode = "401",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if the query cannot be found",
                    responseCode = "404",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if there is an unknown error",
                    responseCode = "500",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class)))})
    // @formatter:on
    @Secured({"Administrator", "JBossAdministrator"})
    @RequestMapping(path = "{id}/adminRemove", method = {RequestMethod.DELETE}, produces = {"application/xml", "text/xml", "application/json", "text/yaml",
            "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public VoidResponse adminRemove(@Parameter(description = "The map reduce query id") @PathVariable String id,
                    @AuthenticationPrincipal DatawaveUserDetails currentUser) throws QueryException {
        return mapReduceQueryManagementService.adminRemove(id, currentUser);
    }
    
    // @formatter:off
    @Operation(
            summary = "Updates the state of the map reduce job.",
            description = "This method is intended to be called by the map reduce job to update the state of the query.")
    @ApiResponses({
            @ApiResponse(
                    description = "if successful, returns a void response indicating that the state was updated",
                    responseCode = "200",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class))),
            @ApiResponse(
                    description = "if there is an unknown error",
                    responseCode = "500",
                    content = @Content(schema = @Schema(implementation = VoidResponse.class)))})
    // @formatter:on
    @RequestMapping(path = "updateState", method = RequestMethod.GET, produces = {"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml",
            "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public VoidResponse updateState(@RequestParam String jobId, @RequestParam String jobStatus) throws QueryException {
        return mapReduceQueryManagementService.updateState(jobId, jobStatus);
    }
}
