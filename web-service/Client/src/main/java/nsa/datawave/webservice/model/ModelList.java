package nsa.datawave.webservice.model;

import java.io.Serializable;
import java.util.HashSet;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

import nsa.datawave.webservice.HtmlProvider;
import nsa.datawave.webservice.result.BaseResponse;

@XmlRootElement(name = "ModelList")
public class ModelList extends BaseResponse implements Serializable, HtmlProvider {
    
    private static final long serialVersionUID = 1L;
    private static final String TITLE = "Model Names", EMPTY = "";
    
    @XmlElementWrapper(name = "ModelNames")
    @XmlElement(name = "ModelName")
    private HashSet<String> names = null;
    
    public HashSet<String> getNames() {
        return names;
    }
    
    public void setNames(HashSet<String> names) {
        this.names = names;
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
        
        if (this.getNames() == null || this.getNames().isEmpty()) {
            builder.append("No models available.");
        } else {
            builder.append("<table>\n");
            
            int x = 0;
            for (String name : this.getNames()) {
                // highlight alternating rows
                if (x % 2 == 0) {
                    builder.append("<tr class=\"highlight\">");
                } else {
                    builder.append("<tr>");
                }
                x++;
                builder.append("<td><a href=\"/DataWave/Model/").append(name).append("\">").append(name).append("</a></td>");
                builder.append("</tr>");
            }
            
            builder.append("</table>\n");
        }
        return builder.toString();
    }
    
}
