package datawave.webservice.result;

import java.util.List;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import datawave.webservice.HtmlProvider;
import datawave.webservice.query.result.logic.QueryLogicDescription;

@XmlRootElement(name = "QueryWizardStep1")
@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class QueryWizardStep1Response extends BaseResponse implements HtmlProvider {

    private static final long serialVersionUID = 1L;
    private static final String TITLE = "Query Wizard Step 1", EMPTY = "";
    private static final String HEADER = "<img src=\"/DataWave/doc/images/dwquery_logo.png\" width=\"429px\" height=\"38px\"\n"
                    + " style=\"padding-left: 10px; padding-right: 40px; padding-top: 10px;padding-bottom: 20px\">";

    @XmlElement(name = "QueryLogic")
    private List<QueryLogicDescription> queryLogicList = null;

    public List<QueryLogicDescription> getQueryLogicList() {
        return queryLogicList;
    }

    public void setQueryLogicList(List<QueryLogicDescription> queryLogicList) {
        this.queryLogicList = queryLogicList;
    }

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
        return HEADER;
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

    @Override
    public String getMainContent() {
        StringBuilder builder = new StringBuilder();
        builder.append("<H1>Query Wizard Step 1 - Choose Query Type</H1>");
        builder.append("<FORM id=\"queryform\" action=\"/DataWave/BasicQuery/showQueryWizardStep2\"  method=\"post\" target=\"_self\" enctype=\"application/x-www-form-urlencoded\">");
        builder.append("<br/>");
        builder.append("<select form=\"queryform\" name=\"queryType\" align=\"left\">");

        for (QueryLogicDescription qld : this.queryLogicList) {
            // <option value="LUCENE">LUCENE</option>
            builder.append("<option value=\"").append(qld.getName()).append("\">").append(qld.getName()).append("</option>");
        }

        builder.append("</select><br/><br/>\n");
        builder.append("<input type=\"submit\" value=\"Submit\"  align=\"center\">");
        builder.append("</FORM>");

        return builder.toString();
    }

}
