package datawave.microservice.accumulo.lookup;

import datawave.accumulo.util.security.UserAuthFunctions;
import datawave.marking.MarkingFunctions;
import datawave.marking.SecurityMarking;
import datawave.microservice.accumulo.lookup.config.LookupAuditProperties;
import datawave.microservice.accumulo.lookup.config.LookupProperties;
import datawave.microservice.audit.AuditClient;
import datawave.microservice.authorization.user.ProxiedUserDetails;
import datawave.security.util.ScannerHelper;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.NotFoundQueryException;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.response.LookupResponse;
import datawave.webservice.response.objects.DefaultKey;
import datawave.webservice.response.objects.Entry;
import datawave.webservice.response.objects.KeyBase;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import org.springframework.util.MultiValueMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Provides Accumulo scan results in the form of {@link LookupResponse} for the given inputs (table, row, cf, cq, etc). Optionally supports auditing of lookup
 * requests via autowired {@link AuditClient}
 */
@Service
@ConditionalOnProperty(name = "accumulo.lookup.enabled", havingValue = "true", matchIfMissing = true)
public class LookupService {
    
    public static final String ALLOWED_ENCODING = "[base64, none]";
    
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    private enum Encoding {
        none, base64;
    }
    
    public interface Parameter {
        String TABLE = "table";
        String ROW = "row";
        String ROW_ENCODING = "rowEnc";
        String CF = "colFam";
        String CF_ENCODING = "cfEnc";
        String CQ = "colQual";
        String CQ_ENCODING = "cqEnc";
        String BEGIN_ENTRY = "beginEntry";
        String USE_AUTHS = "useAuthorizations";
        String END_ENTRY = "endEntry";
    }
    
    private final SecurityMarking auditSecurityMarking;
    private final MarkingFunctions markingFunctions;
    private final Connector connection;
    private final LookupAuditProperties lookupAuditProperties;
    private final LookupProperties lookupProperties;
    private final UserAuthFunctions userAuthFunctions;
    
    // Optional, thus using setter injection
    private AuditClient auditor;
    
    //@formatter:off
    @Autowired
    public LookupService(
        @Qualifier("auditLookupSecurityMarking")
        SecurityMarking auditSecurityMarking,
        MarkingFunctions markingFunctions,
        @Qualifier("warehouse")
        Connector connection,
        LookupAuditProperties lookupAuditProperties,
        LookupProperties lookupProperties,
        UserAuthFunctions userAuthFunctions) {
            this.auditSecurityMarking = auditSecurityMarking;
            this.markingFunctions = markingFunctions;
            this.connection = connection;
            this.lookupAuditProperties = lookupAuditProperties;
            this.lookupProperties = lookupProperties;
            this.userAuthFunctions = userAuthFunctions;
            this.auditor = null;
    }
    //@formatter:on
    
    @Autowired(required = false)
    public void setAuditor(AuditClient auditor) {
        this.auditor = auditor;
    }
    
    /**
     * Look up one or more entries in Accumulo by table, row, and optionally colFam and colQual
     *
     * @param table
     *            accumulo table
     * @param row
     *            requested row
     * @param parameters
     *            additional query parameters
     * @param currentUser
     *            user proxy chain associated with the lookup
     *
     * @return datawave.webservice.response.LookupResponse
     * @throws QueryException
     *             on error
     */
    public LookupResponse lookup(String table, String row, MultiValueMap<String,String> parameters, ProxiedUserDetails currentUser) throws QueryException {
        
        final LookupResponse response = new LookupResponse();
        
        try {
            auditSecurityMarking.clear();
            auditSecurityMarking.validate(parameters);
        } catch (IllegalArgumentException e) {
            log.error("Security marking validation failed for query", e);
            throw new BadRequestQueryException(DatawaveErrorCode.SECURITY_MARKING_CHECK_ERROR, e);
        }
        
        final String rowEnc = parameters.getFirst(Parameter.ROW_ENCODING);
        
        String colFam = parameters.getFirst(Parameter.CF);
        final String cfEnc = parameters.getFirst(Parameter.CF_ENCODING);
        
        String colQual = parameters.getFirst(Parameter.CQ);
        final String cqEnc = parameters.getFirst(Parameter.CQ_ENCODING);
        
        final String beginEntryStr = parameters.getFirst(Parameter.BEGIN_ENTRY);
        final String useAuthorizations = parameters.getFirst(Parameter.USE_AUTHS);
        
        Integer beginEntry = 0;
        if (StringUtils.isNotBlank(beginEntryStr)) {
            beginEntry = Integer.valueOf(beginEntryStr);
        }
        final String endEntryStr = parameters.getFirst(Parameter.END_ENTRY);
        Integer endEntry = Integer.MAX_VALUE;
        if (StringUtils.isNotBlank(endEntryStr)) {
            endEntry = Integer.valueOf(endEntryStr);
        }
        
        if (rowEnc != null) {
            if (Encoding.valueOf(rowEnc.toLowerCase()) == Encoding.base64) {
                row = base64Decode(row);
            } else {
                response.addException(new IllegalArgumentException(
                                String.format("Query parameter \"%s\" must be one of %s", Parameter.ROW_ENCODING, ALLOWED_ENCODING)));
            }
        }
        
        if (colFam != null && cfEnc != null) {
            if (Encoding.valueOf(cfEnc.toLowerCase()) == Encoding.base64) {
                colFam = base64Decode(colFam);
            } else if (Encoding.valueOf(cfEnc.toLowerCase()) != Encoding.none) {
                response.addException(new IllegalArgumentException(
                                String.format("Query parameter \"%s\" must be null or one of %s", Parameter.CF_ENCODING, ALLOWED_ENCODING)));
            }
        }
        
        if (colQual != null && cqEnc != null) {
            if (Encoding.valueOf(cqEnc.toLowerCase()) == Encoding.base64) {
                colQual = base64Decode(colQual);
            } else if (Encoding.valueOf(cqEnc.toLowerCase()) != Encoding.none) {
                response.addException(new IllegalArgumentException(
                                String.format("Query parameter \"%s\" must be null or one of %s", Parameter.CQ_ENCODING, ALLOWED_ENCODING)));
            }
        }
        
        if (beginEntry < 0) {
            response.addException(new IllegalArgumentException(String.format("Query parameter \"%s\" cannot be negative", Parameter.BEGIN_ENTRY)));
        }
        
        if (endEntry < 0) {
            response.addException(new IllegalArgumentException(String.format("Query parameter \"%s\" cannot be negative", Parameter.END_ENTRY)));
        }
        
        if (endEntry < beginEntry) {
            response.addException(new IllegalArgumentException(
                            String.format("Query parameter \"%s\" cannot be smaller than \"%s\"", Parameter.END_ENTRY, Parameter.BEGIN_ENTRY)));
        }
        
        if (useAuthorizations != null) {
            try {
                new Authorizations(useAuthorizations.getBytes());
            } catch (IllegalArgumentException e) {
                response.addException(new IllegalArgumentException(String.format("Invalid argument %s for \"%s\"", useAuthorizations, Parameter.USE_AUTHS)));
            }
        }
        
        final List<?> exceptionList = response.getExceptions();
        if (exceptionList != null && exceptionList.size() > 0) {
            exceptionList.stream().forEach(ex -> log.error(ex.toString()));
            throw new QueryException(DatawaveErrorCode.QUERY_SETUP_ERROR, String.format("%s errors were encountered during query setup", exceptionList.size()));
        }
        
        //@formatter:off
        if (null != auditor) {
            final AuditClient.Request auditRequest = new AuditRequest.Builder()
                .withTable(table)
                .withRow(row)
                .withCF(colFam)
                .withCQ(colQual)
                .withAuditConfig(lookupAuditProperties)
                .withParams(parameters)
                .withProxiedUserDetails(currentUser)
                .withMarking(auditSecurityMarking)
                .build();
            try {
                auditor.submit(auditRequest);
            } catch (Throwable t) {
                log.error("Lookup query audit failed", t);
                throw new QueryException(DatawaveErrorCode.AUDITING_ERROR, t);
            }
        }
        //@formatter:on
        
        BatchScanner batchScanner = null;
        Set<Authorizations> mergedAuths = null;
        
        final List<Entry> entryList = new ArrayList<>();
        
        try {
            final Authorizations primaryUserAuths = userAuthFunctions.getRequestedAuthorizations(useAuthorizations, currentUser.getPrimaryUser());
            mergedAuths = userAuthFunctions.mergeAuthorizations(primaryUserAuths, currentUser.getProxiedUsers(), u -> u != currentUser.getPrimaryUser());
            batchScanner = ScannerHelper.createBatchScanner(connection, table, mergedAuths, lookupProperties.getNumQueryThreads());
            
            final List<Range> ranges = new ArrayList<>();
            
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
            
            final Iterator<Map.Entry<Key,Value>> itr = batchScanner.iterator();
            
            int currEntry = -1;
            while (itr.hasNext()) {
                
                currEntry++;
                Map.Entry<Key,Value> entry = itr.next();
                
                if (currEntry < beginEntry) {
                    continue;
                }
                
                final Key k = entry.getKey();
                final Value v = entry.getValue();
                final String currRow = k.getRow().toString();
                final String currCf = k.getColumnFamily().toString();
                final String currCq = k.getColumnQualifier().toString();
                
                if (!currRow.equals(row)) {
                    continue;
                }
                if (colFam != null && !currCf.equals(colFam)) {
                    continue;
                }
                if (colQual != null && !currCq.equals(colQual)) {
                    continue;
                }
                
                final Map<String,String> markings = getMarkings(k.getColumnVisibility(), mergedAuths);
                
                final KeyBase responseKey = new DefaultKey();
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
            throw new NotFoundQueryException(e);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            response.addException(e);
            throw new QueryException(DatawaveErrorCode.QUERY_EXECUTION_ERROR, e);
        } finally {
            if (batchScanner != null) {
                batchScanner.close();
            }
            
            HashSet<String> mergedAuthsSet = new HashSet<>();
            if (null != mergedAuths) {
                for (Authorizations authorizations : mergedAuths)
                    mergedAuthsSet.addAll(Arrays.asList(authorizations.toString().split(",")));
            }
        }
        
        response.setEntries(entryList);
        
        return response;
    }
    
    /**
     * Translates col viz text into a more portable map representation as desired. Here, the configured MarkingFunction instance is used to perform the
     * translation.
     * 
     * @param colViz
     *            Visibility expression to be translated
     * @param auths
     *            Authorizations, which may or may not inform/impact the translation
     * @return translated col viz param into map form
     * @throws MarkingFunctions.Exception
     *             on error
     */
    protected Map<String,String> getMarkings(Text colViz, Collection<Authorizations> auths) throws MarkingFunctions.Exception {
        return markingFunctions.translateFromColumnVisibilityForAuths(new ColumnVisibility(colViz), auths);
    }
    
    /**
     * Request and builder extensions for lookup-specific audits
     */
    private static class AuditRequest extends AuditClient.Request {
        
        private AuditRequest(Builder b) {
            super(b);
        }
        
        private static class Builder extends AuditClient.Request.Builder {
            
            private String table;
            private String row;
            private String colFam;
            private String colQual;
            private LookupAuditProperties auditConfig;
            
            public Builder withTable(String table) {
                this.table = table;
                return this;
            }
            
            Builder withRow(String row) {
                this.row = row;
                return this;
            }
            
            Builder withCF(String cf) {
                this.colFam = cf;
                return this;
            }
            
            Builder withCQ(String cq) {
                this.colQual = cq;
                return this;
            }
            
            Builder withAuditConfig(LookupAuditProperties auditConfig) {
                this.auditConfig = auditConfig;
                return this;
            }
            
            @Override
            public AuditClient.Request build() {
                // create base query
                final StringBuffer sb = new StringBuffer();
                sb.append("lookup/").append(this.table).append("/").append(this.row);
                if (this.colFam != null) {
                    sb.append("/").append(this.colFam);
                }
                if (this.colQual != null) {
                    sb.append("/").append(this.colQual);
                }
                withQueryExpression(sb.toString());
                withQueryLogic("AccumuloLookup");
                
                // determine AuditType from config
                withAuditType(this.auditConfig.getDefaultAuditType());
                for (LookupAuditProperties.AuditConfiguration entry : this.auditConfig.getTableConfig()) {
                    if (entry.isMatch(this.table, this.row, this.colFam, this.colQual)) {
                        withAuditType(entry.getAuditType());
                        break;
                    }
                }
                return new LookupService.AuditRequest(this);
            }
        }
    }
    
    static String base64Decode(String value) {
        return null == value ? null : new String(Base64.decodeBase64(value.getBytes()));
    }
}
