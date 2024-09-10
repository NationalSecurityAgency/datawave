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

@XmlRootElement(name = "QueryLogicResponse")
@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class QueryLogicResponse extends BaseResponse implements HtmlProvider {

    private static final long serialVersionUID = 1L;
    private static final String TITLE = "Deployed Query Logic", EMPTY = "";

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
        builder.append("<table>\n");

        builder.append("<tr><th>Name</th><th>Description</th><th>Optional Parameters</th><th>Required Parameters</th><th>Example Queries</th><th>Response Class</th><th>AuditType</th><th>Query Syntax</th></tr>");

        final String html = ".html";
        final String javadocs = "/DataWave/doc/javadocs/";

        int x = 0;
        for (QueryLogicDescription qld : this.queryLogicList) {
            // highlight alternating rows
            if (x % 2 == 0) {
                builder.append("<tr class=\"highlight\">");
            } else {
                builder.append("<tr>");
            }
            x++;
            builder.append("<td>").append(qld.getName()).append("</td>");
            builder.append("<td>").append(qld.getLogicDescription()).append("</td>");
            builder.append("<td>").append(StringUtils.join(qld.getSupportedParams(), "<br>")).append("</td>");
            builder.append("<td>").append(StringUtils.join(qld.getRequiredParams(), "<br>")).append("</td>");
            builder.append("<td>").append(StringUtils.join(qld.getExampleQueries(), "<br>")).append("</td>");

            String responseClassName = qld.getResponseClass();
            responseClassName = responseClassName.replaceAll("\\.", "/");
            String url = javadocs + responseClassName + html;
            String label = responseClassName.substring(responseClassName.lastIndexOf("/") + 1);

            builder.append("<td><a href='").append(url).append("'>" + label + "</a></td>");

            builder.append("<td>").append(qld.getAuditType()).append("</td>");
            builder.append("<td>").append(StringUtils.join(qld.getQuerySyntax(), "<br>")).append("</td>");
            builder.append("</tr>");
        }

        builder.append("</table>\n");

        return builder.toString();
    }

}
