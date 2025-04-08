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

@XmlRootElement(name = "QueryWizardStep2")
@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class QueryWizardStep2Response extends BaseResponse implements HtmlProvider {

    private static final long serialVersionUID = 1L;
    private static final String TITLE = "Query Wizard Step 2", EMPTY = "";
    private static final String HEADER = "<img src=\"/DataWave/doc/images/dwquery_logo.png\" width=\"429px\" height=\"38px\"\n"
                    + " style=\"padding-left: 10px; padding-right: 40px; padding-top: 10px;padding-bottom: 20px\">";
    private QueryLogicDescription theQLD = null;
    private String authString = "";
    private String visibilityString = "";

    @XmlElement(name = "QueryLogic")
    private List<QueryLogicDescription> queryLogicList = null;

    public void setTheQueryLogicDescription(QueryLogicDescription queryLogicDescription) {
        this.theQLD = queryLogicDescription;
    }

    public void setAuthString(String authString) {
        this.authString = authString;
        if (!authString.isEmpty() && authString != null) {
            visibilityString = authString.replaceAll(",", "|");
        }
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
        builder.append("<H1>DataWave Query Form</H1>");
        builder.append("<FORM id=\"queryform\" action=\"/DataWave/BasicQuery/" + theQLD.getName()
                        + "/showQueryWizardStep3\"  method=\"post\" target=\"_self\" enctype=\"application/x-www-form-urlencoded\">");
        builder.append("<br/>");
        builder.append("<br/>");
        builder.append("<H2>" + theQLD.getName() + " (" + theQLD.getLogicDescription() + ")</H2>");
        builder.append("<br/>");
        builder.append("<H2>Required parameters</H2>");
        builder.append("<br/><br/>");
        builder.append("<table>");
        builder.append("<tr><td align=\"left\" width=\"10%\">Query Name:</td><td width=\"90%\"> <input type=\"text\" name=\"queryName\" placeholder=\"Enter value\" align=\"left\" size=\"150\" /></td><tr>");
        builder.append("<tr><td align=\"left\">Query:</td><td><textarea rows=\"4\" cols=\"150\" placeholder=\"DataWave Query (no enclosing quotes needed)\" name=\"query\" ></textarea></td><tr>");

        if (theQLD != null) {
            for (String param : theQLD.getRequiredParams()) {
                if (param.equals("auths")) // This is taken care of below.
                    continue;
                else if (!param.contains("query") && !param.equals("logicName")) { // These were done above
                    builder.append("<tr><td align=\"left\" >" + param + ": " + "</td>");
                    builder.append("<td><input type=\"text\" name=\"" + param + "\" placeholder=\"Enter value\" align=\"left\" align=\"left\" /></td></tr>\n");
                }
            }

            // Show some examples if these were not set dynamically like should have been
            if (authString.isEmpty() || authString == null) {
                authString = "BAR,FOO,PRIVATE,PUBLIC";
                visibilityString = authString.replaceAll(",", "|");
            }

            /* QueryParametersImpl.java line 125-130 require these next four input params */
            builder.append("<tr><td align=\"left\" >auths:</td><td><input type=\"text\" name=\"auths\" placeholder=\"" + authString + "\" value=\"" + authString
                            + "\"  size=\"150\" /></td></tr>\n");
            builder.append("<tr><td align=\"left\">Visibility:</td><td><input type=\"text\" name=\"columnVisibility\" placeholder=\"" + visibilityString
                            + "\" value=\"" + visibilityString + "\" align=\"left\"  size=\"150\"/></td></tr>\n");
            builder.append("</table>\n");
            builder.append("<input type=\"hidden\" name=\"param\" value=\"stats=false\" />");
            builder.append("<input type=\"hidden\" name=\"expirationDate\" value=\"20990101\" />");

            builder.append("<br/><br/>\n");

            builder.append("<H2>" + theQLD.getName() + " Supported parameters</H2>");
            builder.append("<table>");

            for (String optional : theQLD.getSupportedParams()) {
                if (optional.equals("query.syntax")) {
                    builder.append("<tr><td>Query Syntax: </td>");
                    builder.append("<td><select form=\"queryform\" name=\"query.syntax\" align=\"left\">");

                    for (String syntax : theQLD.getQuerySyntax()) {
                        builder.append("<option value=\"").append(syntax).append("\">").append(syntax).append("</option>");
                    }
                    builder.append("</select></td></tr>\n");
                } else {
                    builder.append("<tr><td align=\"left\" > " + optional + ":</td>");
                    builder.append("<td><input type=\"text\" name=\"" + optional + "\" value=\"\" align=\"left\" width=\"50\"/></td></tr>\n");
                }
            }

            builder.append("</table>\n");
            builder.append("<br/><br/>\n");
        }

        builder.append("<input type=\"submit\" value=\"Submit\"  align=\"center\">");
        builder.append("</FORM>");

        /*
         * if (theQLD != null) { // There aren't any example queries so this doesn't look good in the UI builder.append("Example Queries: ");
         * builder.append("<br/><br/><table><tr><th>Example</th></tr>");
         *
         * for (String example : theQLD.getExampleQueries()) { builder.append("<tr><td>" + example + "</td></tr>"); }
         *
         * } builder.append("</table>");
         */

        return builder.toString();
    }

}
