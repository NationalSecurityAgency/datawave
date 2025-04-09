package datawave.microservice.accumulo.lookup;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import com.google.common.base.Preconditions;

import datawave.accumulo.util.security.UserAuthFunctions;
import datawave.marking.MarkingFunctions;
import datawave.marking.SecurityMarking;
import datawave.microservice.accumulo.lookup.config.LookupAuditProperties;
import datawave.microservice.accumulo.lookup.config.LookupProperties;
import datawave.microservice.accumulo.lookup.config.ResponseObjectFactory;
import datawave.microservice.audit.AuditClient;
import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.security.util.ScannerHelper;
import datawave.webservice.common.audit.AuditParameters;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.NotFoundQueryException;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.response.LookupResponse;
import datawave.webservice.response.objects.Entry;
import datawave.webservice.response.objects.KeyBase;

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
        none, base64
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
    private final AccumuloClient connection;
    private final LookupAuditProperties lookupAuditProperties;
    private final LookupProperties lookupProperties;
    private final UserAuthFunctions userAuthFunctions;
    private final ResponseObjectFactory responseObjectFactory;
    
    // Optional, thus using setter injection
    private AuditClient auditor;
    
    //@formatter:off
    @Autowired
    public LookupService(
        @Qualifier("auditLookupSecurityMarking")
        SecurityMarking auditSecurityMarking,
        MarkingFunctions markingFunctions,
        @Qualifier("warehouse")
        AccumuloClient connection,
        LookupAuditProperties lookupAuditProperties,
        LookupProperties lookupProperties,
        UserAuthFunctions userAuthFunctions,
        ResponseObjectFactory responseObjectFactory) {
            this.auditSecurityMarking = auditSecurityMarking;
            this.markingFunctions = markingFunctions;
            this.connection = connection;
            this.lookupAuditProperties = lookupAuditProperties;
            this.lookupProperties = lookupProperties;
            this.userAuthFunctions = userAuthFunctions;
            this.responseObjectFactory = responseObjectFactory;
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
     * @param request
     *            lookup request
     * @param currentUser
     *            user proxy chain associated with the lookup
     *            
     * @return datawave.webservice.response.LookupResponse
     * @throws QueryException
     *             on error
     */
    public LookupResponse lookup(LookupRequest request, DatawaveUserDetails currentUser) throws QueryException {
        
        Preconditions.checkNotNull(request, "Request argument cannot be null");
        Preconditions.checkNotNull(currentUser, "User argument cannot be null");
        
        final LookupResponse response = responseObjectFactory.createLookupResponse();
        
        validateRequest(request, response);
        
        //@formatter:off
        if (null != auditor) {
            final AuditClient.Request auditRequest = new AuditRequest.Builder()
                .withTable(request.table)
                .withRow(request.row)
                .withCF(request.colFam)
                .withCQ(request.colQual)
                .withAuths(request.auths)
                .withAuditConfig(lookupAuditProperties)
                .withParams(request.params)
                .withDatawaveUserDetails(currentUser)
                .withMarking(auditSecurityMarking)
                .build();
            try {
                auditor.submit(auditRequest);
            } catch (Exception e) {
                log.error("Lookup query audit failed", e);
                throw new QueryException(DatawaveErrorCode.AUDITING_ERROR, e);
            }
        }
        //@formatter:on
        
        BatchScanner batchScanner = null;
        Set<Authorizations> mergedAuths = null;
        
        final List<Entry> entryList = new ArrayList<>();
        
        try {
            final Authorizations primaryUserAuths = userAuthFunctions.getRequestedAuthorizations(request.auths, currentUser.getPrimaryUser());
            mergedAuths = userAuthFunctions.mergeAuthorizations(primaryUserAuths, currentUser.getProxiedUsers(), u -> u != currentUser.getPrimaryUser());
            batchScanner = ScannerHelper.createBatchScanner(connection, request.table, mergedAuths, lookupProperties.getNumQueryThreads());
            
            final List<Range> ranges = new ArrayList<>();
            
            Key begin;
            Key end;
            if (request.colFam == null && request.colQual == null) {
                begin = new Key(new Text(request.row));
                end = begin.followingKey(PartialKey.ROW);
            } else if (request.colQual == null) {
                begin = new Key(new Text(request.row), new Text(request.colFam));
                end = begin.followingKey(PartialKey.ROW_COLFAM);
            } else {
                begin = new Key(new Text(request.row), new Text(request.colFam), new Text(request.colQual));
                end = begin.followingKey(PartialKey.ROW_COLFAM_COLQUAL);
            }
            ranges.add(new Range(begin, true, end, true));
            batchScanner.setRanges(ranges);
            
            final Iterator<Map.Entry<Key,Value>> itr = batchScanner.iterator();
            
            int currEntry = -1;
            while (itr.hasNext()) {
                
                currEntry++;
                Map.Entry<Key,Value> entry = itr.next();
                
                if (currEntry < request.beginEntry) {
                    continue;
                }
                
                final Key k = entry.getKey();
                final Value v = entry.getValue();
                final String currRow = k.getRow().toString();
                final String currCf = k.getColumnFamily().toString();
                final String currCq = k.getColumnQualifier().toString();
                
                if (!currRow.equals(request.row)) {
                    continue;
                }
                if (request.colFam != null && !currCf.equals(request.colFam)) {
                    continue;
                }
                if (request.colQual != null && !currCq.equals(request.colQual)) {
                    continue;
                }
                
                //@formatter:off
                final Map<String,String> markings = markingFunctions.translateFromColumnVisibilityForAuths(
                    new ColumnVisibility(k.getColumnVisibility()),
                    mergedAuths
                );
                //@formatter:on
                
                final KeyBase responseKey = responseObjectFactory.createKey();
                responseKey.setRow(currRow);
                responseKey.setColFam(currCf);
                responseKey.setColQual(currCq);
                responseKey.setMarkings(markings);
                responseKey.setTimestamp(k.getTimestamp());
                entryList.add(responseObjectFactory.createEntry(responseKey, v.get()));
                
                if (currEntry == request.endEntry) {
                    break;
                }
            }
        } catch (TableNotFoundException e) {
            response.addException(e);
            throw new NotFoundQueryException(e);
        } catch (Exception e) {
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
            
            if (null != response.getExceptions()) {
                logResponseErrors(response.getExceptions());
            }
        }
        
        response.setHasResults(!entryList.isEmpty());
        response.setEntries(entryList);
        return response;
    }
    
    private void addEncodeException(LookupResponse response, String encParamName) {
        response.addException(new IllegalArgumentException(String.format("Query parameter \"%s\" should be one of %s", encParamName, ALLOWED_ENCODING)));
    }
    
    private void checkAuditParameters(MultiValueMap<String,String> parameters) throws BadRequestQueryException {
        try {
            auditSecurityMarking.clear();
            auditSecurityMarking.validate(parameters);
        } catch (IllegalArgumentException e) {
            log.error("Security marking validation failed for query", e);
            throw new BadRequestQueryException(DatawaveErrorCode.SECURITY_MARKING_CHECK_ERROR, e);
        }
    }
    
    private void validateRequest(LookupRequest request, LookupResponse response) throws QueryException {
        
        log.debug("Lookup request: {}", request);
        
        if (null != auditor) {
            checkAuditParameters(request.params);
        }
        
        if (request.rowEnc != null) {
            if (Encoding.valueOf(request.rowEnc.toLowerCase()) == Encoding.base64) {
                request.row = base64Decode(request.row);
            } else {
                addEncodeException(response, Parameter.ROW_ENCODING);
            }
        }
        
        if (request.colFam != null && request.cfEnc != null) {
            if (Encoding.valueOf(request.cfEnc.toLowerCase()) == Encoding.base64) {
                request.colFam = base64Decode(request.colFam);
            } else if (Encoding.valueOf(request.cfEnc.toLowerCase()) != Encoding.none) {
                addEncodeException(response, Parameter.CF_ENCODING);
            }
        }
        
        if (request.colQual != null && request.cqEnc != null) {
            if (Encoding.valueOf(request.cqEnc.toLowerCase()) == Encoding.base64) {
                request.colQual = base64Decode(request.colQual);
            } else if (Encoding.valueOf(request.cqEnc.toLowerCase()) != Encoding.none) {
                addEncodeException(response, Parameter.CQ_ENCODING);
            }
        }
        
        if (request.beginEntry < 0) {
            response.addException(new IllegalArgumentException(String.format("Query parameter \"%s\" cannot be negative", Parameter.BEGIN_ENTRY)));
        }
        
        if (request.endEntry < 0) {
            response.addException(new IllegalArgumentException(String.format("Query parameter \"%s\" cannot be negative", Parameter.END_ENTRY)));
        }
        
        if (request.endEntry < request.beginEntry) {
            response.addException(new IllegalArgumentException(
                            String.format("Query parameter \"%s\" cannot be smaller than \"%s\"", Parameter.END_ENTRY, Parameter.BEGIN_ENTRY)));
        }
        
        if (request.auths != null) {
            try {
                new Authorizations(request.auths.getBytes(UTF_8));
            } catch (IllegalArgumentException e) {
                response.addException(new IllegalArgumentException(String.format("Invalid argument %s for \"%s\"", request.auths, Parameter.USE_AUTHS)));
            }
        }
        
        final List<?> exceptionList = response.getExceptions();
        if (exceptionList != null && exceptionList.size() > 0) {
            log.error("Bad request: " + request);
            logResponseErrors(exceptionList);
            throw new QueryException(DatawaveErrorCode.QUERY_SETUP_ERROR, String.format("%s errors were encountered during query setup", exceptionList.size()));
        }
    }
    
    private void logResponseErrors(List<?> exceptionList) {
        exceptionList.forEach(ex -> log.error(ex.toString()));
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
            private String auths;
            private LookupAuditProperties auditConfig;
            
            public Builder withTable(String table) {
                this.table = table;
                return this;
            }
            
            Builder withRow(String row) {
                this.row = row;
                return this;
            }
            
            Builder withAuths(String auths) {
                this.auths = auths;
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
                final StringBuilder sb = new StringBuilder();
                sb.append("lookup/").append(this.table).append("/").append(this.row);
                if (this.colFam != null) {
                    sb.append("/").append(this.colFam);
                }
                if (this.colQual != null) {
                    sb.append("/").append(this.colQual);
                }
                if (null == this.params) {
                    this.params = new LinkedMultiValueMap<>();
                }
                if (StringUtils.isNotBlank(this.auths)) {
                    this.params.set(AuditParameters.QUERY_AUTHORIZATIONS, this.auths);
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
        return null == value ? null : new String(Base64.decodeBase64(value.getBytes(UTF_8)), UTF_8);
    }
    
    public static class LookupRequest {
        
        private String table;
        private String row;
        private String rowEnc;
        private String colFam;
        private String cfEnc;
        private String colQual;
        private String cqEnc;
        private String auths;
        private int beginEntry = 0;
        private int endEntry = Integer.MAX_VALUE;
        private MultiValueMap<String,String> params;
        
        private LookupRequest() {}
        
        public LookupRequest(Builder b) {
            
            this.table = b.table;
            this.row = b.row;
            this.rowEnc = b.rowEnc;
            this.colFam = b.colFam;
            this.cfEnc = b.cfEnc;
            this.colQual = b.colQual;
            this.cqEnc = b.cqEnc;
            this.auths = b.auths;
            this.params = b.params;
            
            if (StringUtils.isNotBlank(b.beginEntry)) {
                this.beginEntry = Integer.parseInt(b.beginEntry);
            }
            if (StringUtils.isNotBlank(b.endEntry)) {
                this.endEntry = Integer.parseInt(b.endEntry);
            }
        }
        
        @Override
        public String toString() {
            return ToStringBuilder.reflectionToString(this).toString();
        }
        
        public static class Builder {
            private String table;
            private String row;
            private String rowEnc;
            private String colFam;
            private String cfEnc;
            private String colQual;
            private String cqEnc;
            private String auths;
            private String beginEntry;
            private String endEntry;
            private MultiValueMap<String,String> params;
            
            public Builder withTable(String table) {
                this.table = table;
                return this;
            }
            
            public Builder withRow(String row) {
                this.row = row;
                return this;
            }
            
            public Builder withRowEnc(String rowEnc) {
                this.rowEnc = rowEnc;
                return this;
            }
            
            public Builder withColFam(String colFam) {
                this.colFam = colFam;
                return this;
            }
            
            public Builder withColFamEnc(String cfEnc) {
                this.cfEnc = cfEnc;
                return this;
            }
            
            public Builder withColQual(String colQual) {
                this.colQual = colQual;
                return this;
            }
            
            public Builder withColQualEnc(String cqEnc) {
                this.cqEnc = cqEnc;
                return this;
            }
            
            public Builder withAuths(String auths) {
                this.auths = auths;
                return this;
            }
            
            public Builder withBeginEntry(String beginEntry) {
                this.beginEntry = beginEntry;
                return this;
            }
            
            public Builder withEndEntry(String endEntry) {
                this.endEntry = endEntry;
                return this;
            }
            
            public Builder withParameters(MultiValueMap<String,String> params) {
                this.params = params;
                return this;
            }
            
            public LookupRequest build() {
                return new LookupRequest(this);
            }
        }
    }
}
