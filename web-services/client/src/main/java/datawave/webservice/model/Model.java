package datawave.webservice.model;

import java.io.Serializable;
import java.text.MessageFormat;
import java.util.TreeSet;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementRef;
import javax.xml.bind.annotation.XmlElementRefs;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlElements;
import javax.xml.bind.annotation.XmlRootElement;

import datawave.webservice.HtmlProvider;
import datawave.webservice.result.BaseResponse;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

@XmlRootElement(name = "Model")
@XmlAccessorType(XmlAccessType.NONE)
public class Model extends BaseResponse implements Serializable, HtmlProvider {
    
    private static final long serialVersionUID = 1L;
    private String jqueryUri;
    private String dataTablesUri;
    private static final String TITLE = "Model Description", EMPTY = "";
    private static final String DATA_TABLES_TEMPLATE = "<script type=''text/javascript'' src=''{0}''></script>\n"
                    + "<script type=''text/javascript'' src=''{1}''></script>\n"
                    + "<script type=''text/javascript''>\n"
                    + "$(document).ready(function() '{' $(''#myTable'').dataTable('{'\"bPaginate\": false, \"aaSorting\": [[3, \"asc\"]], \"bStateSave\": true'}') '}')\n"
                    + "</script>\n";
    
    public Model(String jqueryUri, String datatablesUri) {
        this.jqueryUri = jqueryUri;
        this.dataTablesUri = datatablesUri;
        
    }
    
    // Only used in ModelBeanTest now
    public Model() {};
    
    @XmlAttribute(name = "name", required = true)
    private String name = null;
    
    @XmlElementWrapper(name = "Mappings")
    @XmlElements({@XmlElement(name = "FieldMapping", type = FieldMapping.class), @XmlElement(name = "PhraseMapping", type = PhraseMapping.class)})
    private TreeSet<Mapping> fields = new TreeSet<Mapping>();
    
    public String getName() {
        return name;
    }
    
    public void setName(String name) {
        this.name = name;
    }
    
    public TreeSet<Mapping> getFields() {
        return fields;
    }
    
    public void setFields(TreeSet<Mapping> fields) {
        this.fields = fields;
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37).append(name).append(fields).toHashCode();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        if (obj == this)
            return true;
        if (obj.getClass() != this.getClass())
            return false;
        Model other = (Model) obj;
        return new EqualsBuilder().append(name, other.name).append(fields, other.fields).isEquals();
    }
    
    @Override
    public String toString() {
        return new ToStringBuilder(this).append("name", name).append("fields", fields).toString();
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
        return TITLE;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see datawave.webservice.HtmlProvider#getHeadContent()
     */
    @Override
    public String getHeadContent() {
        return MessageFormat.format(DATA_TABLES_TEMPLATE, jqueryUri, dataTablesUri);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see datawave.webservice.HtmlProvider#getMainContent()
     */
    @Override
    public String getMainContent() {
        StringBuilder builder = new StringBuilder();
        
        builder.append("<div>\n");
        builder.append("<div id=\"myTable_wrapper\" class=\"dataTables_wrapper no-footer\">\n");
        builder.append("<table id=\"myTable\" class=\"dataTable no-footer\" role=\"grid\" aria-describedby=\"myTable_info\">\n");
        
        builder.append("<thead><tr><th>Visibility</th><th>FieldName</th><th>DataType</th><th>ModelFieldName</th><th>Direction</th></tr></thead>");
        builder.append("<tbody>");
        
        for (Mapping f : this.getFields()) {
            f.appendFields(builder);
            
        }
        
        builder.append("</tbody>");
        builder.append("  </table>\n");
        builder.append("  <div class=\"dataTables_info\" id=\"myTable_info\" role=\"status\" aria-live=\"polite\"></div>\n");
        builder.append("</div>\n");
        builder.append("</div>");
        
        return builder.toString();
    }
    
}
