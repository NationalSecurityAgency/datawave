package nsa.datawave.security.cache;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import org.apache.commons.lang.StringEscapeUtils;

import nsa.datawave.security.util.DnUtils;
import nsa.datawave.webservice.HtmlProvider;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class DnList implements HtmlProvider {
    private static final String TITLE = "Credentials", EMPTY = "";
    
    @XmlElement(name = "dn")
    private List<String> dns;
    
    @XmlTransient
    private Map<String,CacheEntry<String,Principal>> dnEntries;
    
    @SuppressWarnings("unused")
    public DnList() {
        dns = Collections.emptyList();
    }
    
    public DnList(String... dns) {
        this.dns = Arrays.asList(dns);
    }
    
    public DnList(Map<String,CacheEntry<String,Principal>> dnEntries) {
        this.dnEntries = dnEntries;
        this.dns = new ArrayList<>(dnEntries.keySet());
    }
    
    public List<String> getDns() {
        return dns;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("DNs [\n");
        for (String dn : dns) {
            sb.append("  ").append(dn).append("\n");
        }
        sb.append("]");
        return sb.toString();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see nsa.datawave.webservice.HtmlProvider#getTitle()
     */
    @Override
    public String getTitle() {
        return TITLE;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see nsa.datawave.webservice.HtmlProvider#getPageHeader()
     */
    @Override
    public String getPageHeader() {
        return getTitle();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see nsa.datawave.webservice.HtmlProvider#getHeadContent()
     */
    @Override
    public String getHeadContent() {
        return EMPTY;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see nsa.datawave.webservice.HtmlProvider#getMainContent()
     */
    @Override
    public String getMainContent() {
        StringBuilder builder = new StringBuilder();
        builder.append("<table>\n");
        builder.append("<thead><tr><th>DN</th><th>Created</th><th>Expires</th><th></th></tr></thead><tbody>");
        int x = 0;
        
        for (String dn : this.getDns()) {
            // highlight alternating rows
            if (x % 2 == 0) {
                builder.append("<tr class=\"highlight\">");
            } else {
                builder.append("<tr>");
            }
            x++;
            
            builder.append("<td>");
            builder.append("<a href=\"").append(dn).append("/list\">");
            String[] subDns = DnUtils.splitProxiedSubjectIssuerDNs(dn);
            for (int i = 0; i < subDns.length; i += 2) {
                if (i > 0)
                    builder.append("<br/>&#160;&#160;&#160;&#160;");
                builder.append(StringEscapeUtils.escapeHtml(subDns[i]));
                builder.append(" (issuer: ").append(StringEscapeUtils.escapeHtml(subDns[i + 1])).append(")");
            }
            builder.append("</a>").append("</td>");
            InternalCacheEntry<String,Principal> cacheEntry = getCacheEntry(dn);
            builder.append("<td>");
            // append created time
            if (cacheEntry != null)
                builder.append(new Date(cacheEntry.getCreated()));
            builder.append("</td>");
            builder.append("<td>");
            // append expiry time
            if (cacheEntry != null)
                builder.append(new Date(cacheEntry.getExpiryTime()));
            builder.append("</td>");
            builder.append("<td>").append("<a href=\"").append(dn).append("/evict\">").append("evict").append("</a>").append("</td>\n");
            builder.append("</tr>");
        }
        
        builder.append("</tbody></table>");
        
        return builder.toString();
    }
    
    private InternalCacheEntry<String,Principal> getCacheEntry(String dn) {
        InternalCacheEntry<String,Principal> cacheEntry = null;
        if (dnEntries != null) {
            CacheEntry<String,Principal> entry = dnEntries.get(dn);
            if (entry != null && (entry instanceof InternalCacheEntry)) {
                cacheEntry = (InternalCacheEntry<String,Principal>) entry;
            }
        }
        return cacheEntry;
    }
}
