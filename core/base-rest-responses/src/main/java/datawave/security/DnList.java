package datawave.security;

import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import org.apache.commons.text.StringEscapeUtils;

import datawave.microservice.security.util.DnUtils;
import datawave.security.authorization.DatawaveUserInfo;
import datawave.webservice.HtmlProvider;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class DnList implements HtmlProvider {
    private static final String TITLE = "Credentials", EMPTY = "";
    
    @XmlElement(name = "dn")
    private Collection<String> dns;
    
    @XmlTransient
    private Map<String,? extends DatawaveUserInfo> userInfos;
    
    @SuppressWarnings("unused")
    public DnList() {
        dns = Collections.emptyList();
    }
    
    public DnList(List<String> dns) {
        this.dns = dns;
    }
    
    public DnList(Collection<? extends DatawaveUserInfo> users) {
        this.userInfos = users.stream().collect(Collectors.toMap(i -> i.getDn().toString(), Function.identity()));
        this.dns = this.userInfos.keySet();
    }
    
    public Collection<String> getDns() {
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
     * @see datawave.webservice.HtmlProvider#getTitle()
     */
    @Override
    public String getTitle() {
        return TITLE;
    }
    
    /*
     * (non-Javadoc)
     *
     * @see datawave.webservice.HtmlProvider#getPageHeader()
     */
    @Override
    public String getPageHeader() {
        return getTitle();
    }
    
    /*
     * (non-Javadoc)
     *
     * @see datawave.webservice.HtmlProvider#getHeadContent()
     */
    @Override
    public String getHeadContent() {
        return EMPTY;
    }
    
    /*
     * (non-Javadoc)
     *
     * @see datawave.webservice.HtmlProvider#getMainContent()
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
                builder.append(StringEscapeUtils.escapeHtml4((subDns[i])));
                builder.append(" (issuer: ").append(StringEscapeUtils.escapeHtml4(subDns[i + 1])).append(")");
            }
            builder.append("</a>").append("</td>");
            DatawaveUserInfo u = (userInfos != null) ? userInfos.get(dn) : null;
            // append created time
            builder.append("<td>");
            if (u != null && u.getCreationTime() > 0)
                builder.append(new Date(u.getCreationTime()));
            builder.append("</td>");
            // append expires time
            builder.append("<td>");
            if (u != null && u.getExpirationTime() > 0)
                builder.append(new Date(u.getExpirationTime()));
            builder.append("</td>");
            builder.append("<td>").append("<a href=\"").append(dn).append("/evict\">").append("evict").append("</a>").append("</td>\n");
            builder.append("</tr>");
        }
        
        builder.append("</tbody></table>");
        
        return builder.toString();
    }
}
