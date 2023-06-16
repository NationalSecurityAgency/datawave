package datawave.webservice.query.cache;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

import datawave.webservice.HtmlProvider;

import org.apache.commons.lang.StringEscapeUtils;

@XmlRootElement(name = "RunningQueries")
@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class RunningQueries implements HtmlProvider {

    private static final String TITLE = "Running Queries";
    private static final String EMPTY = "";

    @XmlElementWrapper(name = "Queries")
    @XmlElement(name = "Query")
    private List<String> queries = new ArrayList<String>();

    public List<String> getQueries() {
        return queries;
    }

    public void setQueries(List<String> queries) {
        this.queries = queries;
    }

    @Override
    public String getTitle() {
        return TITLE;
    }

    @Override
    public String getHeadContent() {
        return EMPTY;
    }

    @Override
    public String getPageHeader() {
        return getTitle();
    }

    @Override
    public String getMainContent() {
        StringBuilder builder = new StringBuilder();

        builder.append("<table>\n");
        builder.append("<tr><th>Query Details</th></tr>");

        int x = 0;
        for (String query : this.getQueries()) {
            // highlight alternating rows
            if (x % 2 == 0) {
                builder.append("<tr class=\"highlight\">");
            } else {
                builder.append("<tr>");
            }
            x++;

            builder.append("<td style=\"max-width: 1500px; word-wrap: break-word\">").append(StringEscapeUtils.escapeHtml(query)).append("</td>");
            builder.append("</tr>\n");
        }

        builder.append("</table>\n");

        return builder.toString();
    }

}
