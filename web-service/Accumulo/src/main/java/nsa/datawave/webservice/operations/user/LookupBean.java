package nsa.datawave.webservice.operations.user;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Resource;
import javax.annotation.security.DeclareRoles;
import javax.annotation.security.PermitAll;
import javax.annotation.security.RolesAllowed;
import javax.ejb.EJB;
import javax.ejb.EJBContext;
import javax.ejb.LocalBean;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.ejb.TransactionManagement;
import javax.ejb.TransactionManagementType;
import javax.inject.Inject;
import javax.interceptor.Interceptors;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;

import nsa.datawave.annotation.Required;
import nsa.datawave.configuration.spring.SpringBean;
import nsa.datawave.interceptor.RequiredInterceptor;
import nsa.datawave.interceptor.ResponseInterceptor;
import nsa.datawave.marking.MarkingFunctions;
import nsa.datawave.marking.SecurityMarking;
import nsa.datawave.security.authorization.DatawavePrincipal;
import nsa.datawave.security.util.AuthorizationsUtil;
import nsa.datawave.security.util.ScannerHelper;
import nsa.datawave.webservice.common.audit.AuditBean;
import nsa.datawave.webservice.common.audit.AuditParameters;
import nsa.datawave.webservice.common.audit.Auditor.AuditType;
import nsa.datawave.webservice.common.connection.AccumuloConnectionFactory;
import nsa.datawave.webservice.exception.AccumuloExceptionType;
import nsa.datawave.webservice.exception.AccumuloWebApplicationException;
import nsa.datawave.webservice.exception.BadRequestException;
import nsa.datawave.webservice.exception.NotFoundException;
import nsa.datawave.webservice.operations.configuration.LookupAuditConfiguration;
import nsa.datawave.webservice.operations.configuration.LookupBeanConfiguration;
import nsa.datawave.webservice.query.exception.BadRequestQueryException;
import nsa.datawave.webservice.query.exception.DatawaveErrorCode;
import nsa.datawave.webservice.query.result.event.ResponseObjectFactory;
import nsa.datawave.webservice.response.LookupResponse;
import nsa.datawave.webservice.response.objects.Entry;
import nsa.datawave.webservice.response.objects.KeyBase;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.jboss.resteasy.specimpl.MultivaluedMapImpl;

import com.google.common.collect.LinkedHashMultimap;

@Path("/Accumulo")
@RolesAllowed({"InternalUser", "Administrator"})
@DeclareRoles({"InternalUser", "Administrator"})
@LocalBean
@Stateless
@TransactionAttribute(TransactionAttributeType.NOT_SUPPORTED)
@TransactionManagement(TransactionManagementType.BEAN)
@Interceptors({RequiredInterceptor.class, ResponseInterceptor.class})
public class LookupBean {
    
    private enum Encoding {
        none, base64
    };
    
    private static Logger log = Logger.getLogger(LookupBean.class);
    
    @Resource
    private EJBContext ctx;
    
    @EJB
    private AccumuloConnectionFactory connectionFactory;
    
    @EJB
    private AuditBean auditor;
    
    @Inject
    private SecurityMarking marking;
    
    @Inject
    private AuditParameters auditParameters;
    
    @Inject
    private ResponseObjectFactory responseObjectFactory;
    
    @Inject
    @SpringBean(refreshable = true)
    private LookupBeanConfiguration lookupBeanConfiguration;
    
    @Inject
    @SpringBean(refreshable = true)
    private MarkingFunctions markingFunctions;
    
    public LookupBean() {}
    
    /**
     * Look up one or more entries in Accumulo by table, row, and optionally colFam and colQual
     * 
     * @RequestHeader X-ProxiedEntitiesChain (optional) for server calls on behalf of a user: &lt;subjectDN&gt;
     * @RequestHeader X-ProxiedIssuersChain (optional unless X-ProxiedEntitesChain is specified) contains one &lt;issuerDN&gt; per &lt;subjectDN&gt; in
     *                X-ProxiedEntitesChain
     * @param table
     *            table in Accumulo
     * @param row
     *            requested row
     *
     * @HTTP 200 Success
     * @HTTP 400 Missing required parameter(s) or invalid parameter(s)
     * @HTTP 401 AccumuloSecurityException
     * @HTTP 404 Table not found
     * @HTTP 500 AccumuloException
     * @return nsa.datawave.webservice.response.LookupResponse
     */
    @Path("/Lookup/{table}/{row}")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml"})
    @GET
    public LookupResponse lookupGet(@Required("table") @PathParam("table") String table, @Required("row") @PathParam("row") String row, @Context UriInfo ui) {
        
        MultivaluedMap<String,String> queryParameters = ui.getQueryParameters(true);
        
        // see javadoc on deduplicateQueryParameters for why we are doing this
        return lookup(table, row, deduplicateQueryParameters(queryParameters));
    }
    
    /**
     * Takes the queryParameters and removes duplicates values in each key.
     * <p>
     * This does not remove duplicate values across keys, only duplicates that exist for a given key.
     * <p>
     * When UriInfo is injected into a call, like lookupGet, the query parameters are parsed twice. This results in each key have duplicate values. See
     * RESTEASY-1308 which is fixed in RESTEASY-1331 and merged into resteasy-jaxrs 3.0.17.
     * <p>
     * As a workaround until we upgrade, let's remove the duplication.
     * 
     * @param queryParameters
     * @return MultivalueMap<String, String> with not duplicate values
     */
    
    public static MultivaluedMap<String,String> deduplicateQueryParameters(MultivaluedMap<String,String> queryParameters) {
        MultivaluedMap<String,String> fixedQueryParameters = new MultivaluedMapImpl<>();
        
        LinkedHashMultimap<String,String> lhmm = LinkedHashMultimap.create();
        for (Map.Entry<String,List<String>> entry : queryParameters.entrySet()) {
            // adding them will deplicate
            lhmm.putAll(entry.getKey(), entry.getValue());
        }
        // put them back into a MultivaluedMap
        for (Map.Entry<String,String> entry : lhmm.entries()) {
            fixedQueryParameters.add(entry.getKey(), entry.getValue());
        }
        
        return fixedQueryParameters;
    }
    
    /**
     * Look up one or more entries in Accumulo by table, row, and optionally colFam and colQual
     * 
     * @RequestHeader X-ProxiedEntitiesChain (optional) for server calls on behalf of a user: &lt;subjectDN&gt;
     * @RequestHeader X-ProxiedIssuersChain (optional unless X-ProxiedEntitesChain is specified) contains one &lt;issuerDN&gt; per &lt;subjectDN&gt; in
     *                X-ProxiedEntitesChain
     * @param table
     *            table in Accumulo
     * @param row
     *            requested row
     *
     * @HTTP 200 Success
     * @HTTP 400 Missing required parameter(s) or invalid parameter(s)
     * @HTTP 401 AccumuloSecurityException
     * @HTTP 404 Table not found
     * @HTTP 500 AccumuloException
     * @return nsa.datawave.webservice.response.LookupResponse
     */
    @Path("/Lookup/{table}/{row}")
    @Consumes("application/x-www-form-urlencoded")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml"})
    @POST
    public LookupResponse lookupPost(@Required("table") @PathParam("table") String table, @Required("row") @PathParam("row") String row,
                    MultivaluedMap<String,String> formParameters) {
        
        return lookup(table, row, formParameters);
    }
    
    @PermitAll
    public LookupResponse lookup(String table, String row, MultivaluedMap<String,String> queryParameters) {
        
        LookupResponse response = new LookupResponse();
        
        try {
            marking.clear();
            marking.validate(queryParameters);
        } catch (IllegalArgumentException e) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.SECURITY_MARKING_CHECK_ERROR, e);
            response.addException(qe);
            throw new BadRequestException(qe, response);
        }
        String rowEnc = queryParameters.getFirst("rowEnc");
        String colFam = queryParameters.getFirst("colFam");
        String cfEnc = queryParameters.getFirst("cfEnc");
        String colQual = queryParameters.getFirst("colQual");
        String cqEnc = queryParameters.getFirst("cqEnc");
        String beginEntryStr = queryParameters.getFirst("beginEntry");
        String useAuthorizations = queryParameters.getFirst("useAuthorizations");
        Integer beginEntry = 0;
        if (StringUtils.isNotBlank(beginEntryStr)) {
            beginEntry = Integer.valueOf(beginEntryStr);
        }
        String endEntryStr = queryParameters.getFirst("endEntry");
        Integer endEntry = Integer.MAX_VALUE;
        if (StringUtils.isNotBlank(endEntryStr)) {
            endEntry = Integer.valueOf(endEntryStr);
        }
        
        if (rowEnc != null) {
            if (Encoding.valueOf(rowEnc.toLowerCase()) == Encoding.base64) {
                row = base64Decode(row);
            } else {
                response.addException(new IllegalArgumentException("QueryParam \"rowEnc\" can only be set to \"none\" or \"base64\""));
            }
        }
        
        if (colFam != null && cfEnc != null) {
            if (Encoding.valueOf(cfEnc.toLowerCase()) == Encoding.base64) {
                colFam = base64Decode(colFam);
            } else {
                response.addException(new IllegalArgumentException("QueryParam \"cfEnc\" can only be set to \"none\" or \"base64\""));
            }
        }
        
        if (colQual != null && cqEnc != null) {
            if (Encoding.valueOf(cqEnc.toLowerCase()) == Encoding.base64) {
                colQual = base64Decode(colQual);
            } else {
                response.addException(new IllegalArgumentException("QueryParam \"cqEnc\" can only be set to \"none\" or \"base64\""));
            }
        }
        
        if (beginEntry < 0) {
            response.addException(new IllegalArgumentException("QueryParam \"beginEntry\" cannot be negative"));
        }
        
        if (endEntry < 0) {
            response.addException(new IllegalArgumentException("QueryParam \"endEntry\" cannot be negative"));
        }
        
        if (endEntry < beginEntry) {
            response.addException(new IllegalArgumentException("QueryParam \"endEntry\" cannot be smaller than QueryParam \"beginEntry\""));
        }
        
        if (useAuthorizations != null) {
            try {
                new Authorizations(useAuthorizations.getBytes());
            } catch (IllegalArgumentException e) {
                response.addException(new IllegalArgumentException("Invalid argument " + useAuthorizations + " for \"useAuthorizations\""));
            }
        }
        
        StringBuffer sb = new StringBuffer();
        sb.append("Lookup/").append(table).append("/").append(row);
        if (colFam != null) {
            sb.append("/").append(colFam);
        }
        if (colQual != null) {
            sb.append("/").append(colQual);
        }
        
        AuditType auditType = lookupBeanConfiguration.getDefaultAuditType();
        for (LookupAuditConfiguration entry : lookupBeanConfiguration.getLookupAuditConfiguration()) {
            if (entry.isMatch(table, row, colFam, colQual)) {
                auditType = entry.getAuditType();
                break;
            }
        }
        
        List<AccumuloExceptionType> exceptionList = response.getExceptions();
        if (exceptionList != null && exceptionList.size() > 0) {
            throw new BadRequestException(null, response);
        }
        
        BatchScanner batchScanner = null;
        
        // Find out who/what called this method
        Principal p = ctx.getCallerPrincipal();
        String sid = p.getName();
        String userDn = sid;
        Collection<Collection<String>> cbAuths = new HashSet<>();
        if (p instanceof DatawavePrincipal) {
            DatawavePrincipal cp = (DatawavePrincipal) p;
            sid = cp.getShortName();
            cbAuths.addAll(cp.getAuthorizations());
        }
        
        log.trace(sid + " has authorizations " + cbAuths.toString());
        
        queryParameters.add(AuditParameters.USER_DN, userDn);
        queryParameters.add(AuditParameters.QUERY_STRING, sb.toString());
        queryParameters.add(AuditParameters.QUERY_AUTHORIZATIONS, cbAuths.toString());
        queryParameters.add(AuditParameters.QUERY_AUDIT_TYPE, auditType.toString());
        queryParameters.add(AuditParameters.QUERY_SECURITY_MARKING_COLVIZ, marking.toColumnVisibilityString());
        queryParameters.add("logicClass", "AccumuloLookupBean");
        try {
            auditParameters.clear();
            auditParameters.validate(queryParameters);
        } catch (IllegalArgumentException e) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.SECURITY_MARKING_CHECK_ERROR, e);
            response.addException(qe);
            throw new BadRequestException(qe, response);
        }
        
        Set<Authorizations> mergedAuths = null;
        Connector connection = null;
        List<Entry> entryList = new ArrayList<>();
        
        try {
            Map<String,String> trackingMap = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
            connection = connectionFactory.getConnection(AccumuloConnectionFactory.Priority.NORMAL, trackingMap);
            mergedAuths = AuthorizationsUtil.getDowngradedAuthorizations(useAuthorizations, p);
            
            batchScanner = ScannerHelper.createBatchScanner(connection, table, mergedAuths, 8);
            
            List<Range> ranges = new ArrayList<>();
            Key begin = null;
            Key end = null;
            if (colFam == null && colQual == null) {
                begin = new Key(new Text(row));
                end = begin.followingKey(PartialKey.ROW);
            } else if (colQual == null) {
                begin = new Key(new Text(row), new Text(colFam));
                end = begin.followingKey(PartialKey.ROW_COLFAM);
            } else {
                begin = new Key(new Text(row), new Text(colFam), new Text(colQual));
                end = begin.followingKey(PartialKey.ROW_COLFAM_COLQUAL);
            }
            ranges.add(new Range(begin, true, end, true));
            batchScanner.setRanges(ranges);
            
            Iterator<Map.Entry<Key,Value>> itr = batchScanner.iterator();
            
            int currEntry = -1;
            while (itr.hasNext()) {
                
                currEntry++;
                Map.Entry<Key,Value> entry = itr.next();
                
                if (currEntry < beginEntry) {
                    continue;
                }
                
                Key k = entry.getKey();
                Value v = entry.getValue();
                
                String currRow = k.getRow().toString();
                String currCf = k.getColumnFamily().toString();
                String currCq = k.getColumnQualifier().toString();
                
                if (!currRow.equals(row)) {
                    continue;
                }
                if (colFam != null && !currCf.equals(colFam)) {
                    continue;
                }
                if (colQual != null && !currCq.equals(colQual)) {
                    continue;
                }
                
                Map<String,String> markings = markingFunctions
                                .translateFromColumnVisibilityForAuths(new ColumnVisibility(k.getColumnVisibility()), mergedAuths);
                KeyBase responseKey = responseObjectFactory.getKey();
                responseKey.setRow(currRow);
                responseKey.setColFam(currCf);
                responseKey.setColQual(currCq);
                responseKey.setMarkings(markings);
                responseKey.setTimestamp(k.getTimestamp());
                entryList.add(new Entry(responseKey, v.get()));
                
                if (currEntry == endEntry) {
                    break;
                }
            }
        } catch (TableNotFoundException e) {
            log.info(e.getMessage());
            response.addException(e);
            throw new NotFoundException(e, response);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            response.addException(e);
            throw new AccumuloWebApplicationException(e, response);
        } finally {
            if (batchScanner != null) {
                batchScanner.close();
            }
            
            if (connection != null) {
                try {
                    connectionFactory.returnConnection(connection);
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }
            
            if (auditor == null) {
                throw new BadRequestException(new IllegalArgumentException("Auditor is null, can not process request"), response);
            } else {
                log.info("Auditing Lookup for " + sb.toString() + " at AuditType " + auditType.toString());
                
                if (log.isTraceEnabled()) {
                    log.trace("Auditing Lookup for " + sb.toString() + " at AuditType " + auditType.toString());
                }
                
                Date now = new Date();
                HashSet<String> mergedAuthsSet = new HashSet<>();
                if (null != mergedAuths) {
                    for (Authorizations authorizations : mergedAuths)
                        mergedAuthsSet.addAll(Arrays.asList(authorizations.toString().split(",")));
                }
                try {
                    auditor.audit(auditParameters);
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                    response.addException(e);
                }
            }
        }
        
        response.setEntries(entryList);
        
        return response;
    }
    
    private String base64Decode(String value) {
        if (value == null) {
            return null;
        } else {
            return new String(Base64.decodeBase64(value.getBytes()));
        }
    }
}
