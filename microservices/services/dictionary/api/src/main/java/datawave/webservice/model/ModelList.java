package datawave.webservice.model;

import java.io.Serializable;
import java.text.MessageFormat;
import java.util.HashSet;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

import com.google.common.base.Strings;

import datawave.webservice.HtmlProvider;
import datawave.webservice.result.BaseResponse;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@XmlRootElement(name = "ModelList")
public class ModelList extends BaseResponse implements Serializable, HtmlProvider {
    
    private String jqueryUri;
    private String dataTablesUri;
    private String modelTableName;
    
    private static final long serialVersionUID = 1L;
    private static final String TITLE = "Model Names";
    private static final String DATA_TABLES_TEMPLATE = "<script type=''text/javascript'' src=''{0}''></script>\n"
                    + "<script type=''text/javascript'' src=''{1}''></script>\n" + "<script type=''text/javascript''>\n"
                    + "$(document).ready(function() '{' $(''#myTable'').dataTable('{'\"bPaginate\": false, \"aaSorting\": [[0, \"asc\"]], \"bStateSave\": true'}') '}')\n"
                    + "</script>\n";
    
    public ModelList(String jqueryUri, String datatablesUri, String modelTableName) {
        this.jqueryUri = jqueryUri;
        this.dataTablesUri = datatablesUri;
        this.modelTableName = modelTableName;
    }
    
    @XmlElementWrapper(name = "ModelNames")
    @XmlElement(name = "ModelName")
    private HashSet<String> names;
    
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
        if (!(Strings.isNullOrEmpty(jqueryUri) || Strings.isNullOrEmpty(dataTablesUri))) {
            return MessageFormat.format(DATA_TABLES_TEMPLATE, jqueryUri, dataTablesUri);
        }
        return "";
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see datawave.webservice.HtmlProvider#getMainContent()
     */
    @Override
    public String getMainContent() {
        StringBuilder builder = new StringBuilder();
        
        if (this.getNames() == null || this.getNames().isEmpty()) {
            builder.append("No models available.");
        } else {
            builder.append("<div>\n");
            builder.append("<div id=\"myTable_wrapper\" class=\"dataTables_wrapper no-footer\">\n");
            builder.append("<table id=\"myTable\" class=\"dataTable no-footer\" role=\"grid\" aria-describedby=\"myTable_info\">\n");
            builder.append("<thead><tr><th>Model Name</th></tr></thead>");
            builder.append("<tbody>");
            
            for (String name : this.getNames()) {
                // highlight alternating rows
                builder.append("<tr>");
                builder.append("<td><a href=\"/DataWave/Model/").append(name).append("?modelTableName=").append(modelTableName);
                builder.append("\">").append(name).append("</a></td>");
                builder.append("</tr>\n");
            }
            builder.append("</tbody>");
            builder.append("  </table>\n");
            builder.append("  <div class=\"dataTables_info\" id=\"myTable_info\" role=\"status\" aria-live=\"polite\"></div>\n");
            builder.append("</div>\n");
            builder.append("</div>");
        }
        return builder.toString();
    }
    
}
