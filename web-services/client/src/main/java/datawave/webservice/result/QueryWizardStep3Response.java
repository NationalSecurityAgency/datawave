package datawave.webservice.result;

import java.util.List;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.commons.lang.StringUtils;

import datawave.webservice.HtmlProvider;
import datawave.webservice.query.result.logic.QueryLogicDescription;

@XmlRootElement(name = "QueryWizardStep3")
@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class QueryWizardStep3Response extends BaseResponse implements HtmlProvider {

    private static final long serialVersionUID = 1L;
    private static final String TITLE = "Query Plan", EMPTY = "";
    @XmlElement(name = "plan")
    private String plan = null;

    @XmlElement(name = "queryId")
    private String queryId = null;
    @XmlElement(name = "errorMessage")
    private String errorMessage = null;

    public void setQueryId(String queryId) {
        this.queryId = queryId;
    }

    public void setQueryPlan(String theplan) {
        this.plan = theplan;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
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

    @Override
    public String getMainContent() {
        StringBuilder builder = new StringBuilder();
        builder.append("<h1>DataWave Query Plan</h1>");
        builder.append("<br/>");
        builder.append("<br/>");
        if (plan != null)
            builder.append("<h2>The query plan: " + plan + "</h2>");
        else {
            builder.append("<h2>Datawave could not generate a plan for the query</h2>");
            if (errorMessage != null && !errorMessage.isEmpty()) {
                builder.append("<br><h3>" + errorMessage + "</h3>");
            }
            return builder.toString();
        }

        builder.append("<br/>");
        builder.append("<h2>Results</h2>");
        builder.append("<br/><br/>");
        builder.append("<form id=\"queryform\" action=\"/DataWave/BasicQuery/" + queryId
                        + "/showQueryWizardResults\"  method=\"get\" target=\"_self\" enctype=\"application/x-www-form-urlencoded\">");
        builder.append("<center><input type=\"submit\" value=\"Next\" align=\"left\" width=\"50\" /></center>");

        builder.append("</form>");

        return builder.toString();
    }

}
