package datawave.webservice.common.result;

import java.text.NumberFormat;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.commons.lang.StringEscapeUtils;

import datawave.webservice.HtmlProvider;
import datawave.webservice.result.BaseResponse;

@XmlRootElement(name = "ConnectionFactoryResponse")
@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class ConnectionFactoryResponse extends BaseResponse implements HtmlProvider {

    private static final long serialVersionUID = 1L;
    private static final String TITLE = "Accumulo Connection Factory Metrics", EMPTY = "";

    @XmlElementWrapper(name = "ConnectionPools")
    @XmlElement(name = "ConnectionPool")
    private List<ConnectionPool> connectionPools = new LinkedList<>();

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

        NumberFormat formatter = NumberFormat.getInstance();
        formatter.setGroupingUsed(true);
        formatter.setMaximumFractionDigits(2);
        formatter.setParseIntegerOnly(false);

        StringBuilder builder = new StringBuilder();

        builder.append("<style>\n");

        builder.append("table.connectionPools td, table.connectionPools th {");
        builder.append("border-colapse: collapse;\n");
        builder.append("border: 1px black solid;\n");
        builder.append("empty-cells: hide;\n");
        builder.append("}\n");

        builder.append("\n");

        builder.append("table.connectionRequests td, table.connectionPools th {");
        builder.append("border-colapse: collapse;\n");
        builder.append("border: 1px black solid;\n");
        builder.append("word-wrap: break-word;\n");
        builder.append("empty-cells: hide;\n");
        builder.append("}\n");

        builder.append("</style>\n");

        builder.append("<h2>").append("Connection Pools").append("</h2>");
        builder.append("<br/>");
        builder.append("<table class=\"connectionPools\">");
        builder.append("<tr><th>Pool Name</th><th>Priority</th><th>Num Active</th><th>Max Active</th><th>Num Idle</th><th>Max Idle</th><th>Num Waiting</th></tr>");

        Set<ConnectionPool> poolSet = new TreeSet<>();
        poolSet.addAll(connectionPools);

        for (ConnectionPool f : poolSet) {
            builder.append("<tr>");
            builder.append("<td>").append(f.getPoolName()).append("</td>");
            builder.append("<td>").append(f.getPriority()).append("</td>");
            builder.append("<td>").append(f.getNumActive()).append("</td>");
            builder.append("<td>").append(f.getMaxActive()).append("</td>");
            builder.append("<td>").append(f.getNumIdle()).append("</td>");
            builder.append("<td>").append(f.getMaxIdle()).append("</td>");
            builder.append("<td>").append(f.getNumWaiting()).append("</td>");
            builder.append("</tr>");
        }
        builder.append("</table>");

        builder.append("<br/>");

        builder.append("<h2>").append("ConnectionRequests").append("</h2>");
        builder.append("<table class=\"connectionRequests\">");
        builder.append("<tr><th>Pool Name</th><th>Priority</th><th>State</th><th>Time In State (ms)</th><th>Key</th><th>Value</th></tr>");

        for (ConnectionPool f : connectionPools) {

            List<Connection> connectionRequests = f.getConnectionRequests();

            if (connectionRequests != null) {
                for (Connection h : connectionRequests) {

                    builder.append("<tr>");
                    builder.append("<td>").append(f.getPoolName()).append("</td>");
                    builder.append("<td>").append(f.getPriority()).append("</td>");
                    builder.append("<td>").append(h.getState()).append("</td>");
                    builder.append("<td>").append(h.getTimeInState()).append("</td>");
                    String requestLocation = h.getRequestLocation();
                    if (requestLocation == null) {
                        builder.append("<td></td>");
                        builder.append("<td></td>");
                    } else {
                        builder.append("<td>request.location</td>");
                        builder.append("<td>").append(h.getRequestLocation()).append("</td>");
                    }
                    builder.append("</tr>");

                    Map<String,String> properties = h.getConnectionPropertiesAsMap();
                    String queryUser = properties.get("query.user");

                    for (Map.Entry<String,String> e : properties.entrySet()) {

                        String name = e.getKey();
                        String value = e.getValue();

                        builder.append("<tr>");
                        builder.append("<td></td>");
                        builder.append("<td></td>");
                        builder.append("<td></td>");
                        builder.append("<td></td>");
                        builder.append("<td class=\"allBorders\">").append(StringEscapeUtils.escapeHtml(name)).append("</td>");
                        if (name.equals("query.id") && queryUser != null) {
                            builder.append("<td class=\"allBorders\"><a href=\"/DataWave/Query/Metrics/id/").append(value).append("/").append("\">")
                                            .append(value).append("</a></td>");
                        } else {
                            builder.append("<td class=\"allBorders\">").append(StringEscapeUtils.escapeHtml(e.getValue().toString())).append("</td>");
                        }
                        builder.append("</tr>");
                    }

                }
            }
        }
        builder.append("</table>");

        return builder.toString();
    }

    public List<ConnectionPool> getConnectionFactories() {
        return connectionPools;
    }

    public void setConnectionPools(List<ConnectionPool> connectionPool) {
        this.connectionPools.clear();
        this.connectionPools.addAll(connectionPool);
    }

    public void addConnectionPool(ConnectionPool connectionPool) {
        this.connectionPools.add(connectionPool);
    }
}
