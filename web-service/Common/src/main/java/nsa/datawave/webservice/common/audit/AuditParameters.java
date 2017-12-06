package nsa.datawave.webservice.common.audit;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MultivaluedMap;

import com.google.common.base.Splitter;
import nsa.datawave.validation.ParameterValidator;
import nsa.datawave.webservice.common.audit.Auditor.AuditType;

import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.jboss.resteasy.specimpl.MultivaluedMapImpl;

import com.google.common.base.Preconditions;
import java.util.Collection;

public class AuditParameters implements ParameterValidator {
    
    public static final String USER_DN = "auditUserDN";
    public static final String QUERY_STRING = "query";
    public static final String QUERY_SELECTORS = "selectors";
    public static final String QUERY_AUTHORIZATIONS = "auths";
    public static final String QUERY_AUDIT_TYPE = "auditType";
    public static final String QUERY_SECURITY_MARKING_COLVIZ = "auditColumnVisibility";
    public static final String QUERY_DATE = "queryDate";
    
    private static final List<String> REQUIRED_PARAMS = Arrays.asList(USER_DN, QUERY_STRING, QUERY_AUTHORIZATIONS, QUERY_AUDIT_TYPE,
                    QUERY_SECURITY_MARKING_COLVIZ);
    
    protected Date queryDate = null;
    protected String userDn;
    protected String query = null;
    protected List<String> selectors = null;
    protected String auths = null;
    protected AuditType auditType = null;
    protected ColumnVisibility colviz = null;
    
    public void validate(MultivaluedMap<String,String> parameters) throws IllegalArgumentException {
        this.queryDate = new Date();
        for (String param : REQUIRED_PARAMS) {
            List<String> values = parameters.get(param);
            if (null == values) {
                throw new IllegalArgumentException("Required parameter " + param + " not found");
            }
            if (values.isEmpty() || values.size() > 1) {
                throw new IllegalArgumentException("Required parameter " + param + " only accepts one value");
            }
            switch (param) {
                case USER_DN:
                    this.userDn = values.get(0);
                    break;
                case QUERY_STRING:
                    this.query = values.get(0);
                    break;
                case QUERY_AUTHORIZATIONS:
                    // ensure that auths are comma separated with no empty values or spaces
                    Splitter splitter = Splitter.on(',').omitEmptyStrings().trimResults();
                    this.auths = StringUtils.join(splitter.splitToList(values.get(0)), ",");
                    break;
                case QUERY_AUDIT_TYPE:
                    this.auditType = AuditType.valueOf(values.get(0));
                    break;
                case QUERY_SECURITY_MARKING_COLVIZ:
                    this.colviz = new ColumnVisibility(values.get(0));
                    break;
                default:
                    throw new IllegalArgumentException("Unknown condition.");
            }
        }
        Preconditions.checkNotNull(this.userDn);
        Preconditions.checkNotNull(this.query);
        Preconditions.checkNotNull(this.auths);
        Preconditions.checkNotNull(this.auditType);
        Preconditions.checkNotNull(this.colviz);
    }
    
    public Date getQueryDate() {
        return queryDate;
    }
    
    public void setQueryDate(Date queryDate) {
        this.queryDate = queryDate;
    }
    
    public String getUserDn() {
        return userDn;
    }
    
    public void setUserDn(String userDn) {
        this.userDn = userDn;
    }
    
    public String getQuery() {
        return query;
    }
    
    public void setQuery(String query) {
        this.query = query;
    }
    
    public String getAuths() {
        return auths;
    }
    
    public void setAuths(String auths) {
        // ensure that auths are comma separated with no empty values or spaces
        Splitter splitter = Splitter.on(',').omitEmptyStrings().trimResults();
        this.auths = StringUtils.join(splitter.splitToList(auths), ",");
    }
    
    public AuditType getAuditType() {
        return auditType;
    }
    
    public void setAuditType(AuditType auditType) {
        this.auditType = auditType;
    }
    
    public ColumnVisibility getColviz() {
        return colviz;
    }
    
    public void setColviz(ColumnVisibility colviz) {
        this.colviz = colviz;
    }
    
    public void setSelectors(List<String> selectors) {
        this.selectors = selectors;
    }
    
    public List<String> getSelectors() {
        return selectors;
    }
    
    public Map<String,Object> toMap() {
        Map<String,Object> map = new HashMap<>();
        map.put(QUERY_DATE, this.queryDate.getTime());
        map.put(USER_DN, this.userDn);
        map.put(QUERY_STRING, this.query);
        if (this.selectors != null) {
            map.put(QUERY_SELECTORS, this.selectors);
        }
        map.put(QUERY_AUTHORIZATIONS, this.auths);
        map.put(QUERY_AUDIT_TYPE, this.auditType.name());
        map.put(QUERY_SECURITY_MARKING_COLVIZ, new String(this.colviz.flatten()));
        return map;
    }
    
    protected static MultivaluedMap<String,Object> parseMessage(Map<String,Object> msg) {
        MultivaluedMap<String,Object> p = new MultivaluedMapImpl<>();
        p.put(USER_DN, Collections.singletonList((Object) msg.get(USER_DN)));
        p.put(QUERY_STRING, Collections.singletonList((Object) msg.get(QUERY_STRING)));
        if (msg.containsKey(QUERY_SELECTORS)) {
            p.put(QUERY_SELECTORS, (List<Object>) msg.get(QUERY_SELECTORS));
        }
        p.put(QUERY_AUTHORIZATIONS, Collections.singletonList((Object) msg.get(QUERY_AUTHORIZATIONS)));
        p.put(QUERY_AUDIT_TYPE, Collections.singletonList((Object) msg.get(QUERY_AUDIT_TYPE)));
        p.put(QUERY_SECURITY_MARKING_COLVIZ, Collections.singletonList((Object) msg.get(QUERY_SECURITY_MARKING_COLVIZ)));
        return p;
    }
    
    public AuditParameters fromMap(Map<String,Object> msg) {
        AuditParameters ap = new AuditParameters();
        MultivaluedMap<String,Object> pObj = parseMessage(msg);
        // parseMessage returns MultivaluedMap<String,Object> --> convert to a MultivaluedMap<String,Object> for call to validate()
        MultivaluedMap<String,String> p = new MultivaluedMapImpl<>();
        for (Map.Entry<String,List<Object>> entry : pObj.entrySet()) {
            for (Object o : entry.getValue()) {
                if (o instanceof String) {
                    p.add(entry.getKey(), (String) o);
                }
            }
        }
        ap.validate(p);
        ap.setQueryDate(new Date((long) msg.get(QUERY_DATE)));
        ap.setSelectors((List<String>) msg.get(QUERY_SELECTORS));
        return ap;
    }
    
    @Override
    public String toString() {
        ToStringBuilder tsb = new ToStringBuilder(this);
        tsb.append("userDN").append(this.userDn);
        tsb.append("query").append(this.query);
        tsb.append("selectors").append(this.selectors);
        tsb.append("auths").append(this.auths);
        tsb.append("auditType").append(this.auditType == null ? "unknown" : this.auditType.name());
        tsb.append("colviz").append(this.colviz == null ? "unknown" : this.colviz.toString());
        return tsb.toString();
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        
        AuditParameters that = (AuditParameters) o;
        
        if (queryDate != null ? !queryDate.equals(that.queryDate) : that.queryDate != null)
            return false;
        if (userDn != null ? !userDn.equals(that.userDn) : that.userDn != null)
            return false;
        if (query != null ? !query.equals(that.query) : that.query != null)
            return false;
        if (selectors != null ? !selectors.equals(that.selectors) : that.selectors != null)
            return false;
        if (auths != null ? !auths.equals(that.auths) : that.auths != null)
            return false;
        if (auditType != that.auditType)
            return false;
        return !(colviz != null ? !colviz.equals(that.colviz) : that.colviz != null);
        
    }
    
    @Override
    public int hashCode() {
        int result = queryDate != null ? queryDate.hashCode() : 0;
        result = 31 * result + (userDn != null ? userDn.hashCode() : 0);
        result = 31 * result + (query != null ? query.hashCode() : 0);
        result = 31 * result + (selectors != null ? selectors.hashCode() : 0);
        result = 31 * result + (auths != null ? auths.hashCode() : 0);
        result = 31 * result + (auditType != null ? auditType.hashCode() : 0);
        result = 31 * result + (colviz != null ? colviz.hashCode() : 0);
        return result;
    }
    
    public void clear() {
        this.queryDate = null;
        this.userDn = null;
        this.query = null;
        this.selectors = null;
        this.auths = null;
        this.auditType = null;
        this.colviz = null;
    }
    
    public Collection<String> getRequiredAuditParameters() {
        return REQUIRED_PARAMS;
    }
}
