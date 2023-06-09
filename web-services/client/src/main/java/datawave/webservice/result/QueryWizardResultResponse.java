package datawave.webservice.result;

import datawave.webservice.HtmlProvider;
import datawave.webservice.query.result.EdgeQueryResponseBase;
import datawave.webservice.query.result.edge.EdgeBase;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.FieldBase;
import datawave.webservice.query.result.istat.FieldStat;
import datawave.webservice.query.result.istat.IndexStatsResponse;
import datawave.webservice.query.result.metadata.MetadataFieldBase;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

@XmlRootElement(name = "QueryWizardNextResult")
@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class QueryWizardResultResponse extends BaseResponse implements HtmlProvider {

    private static final long serialVersionUID = 1L;
    private static final String TITLE = "DataWave Query Results", EMPTY = "";
    private static final String DATA_TABLES_TEMPLATE = "<script type=''text/javascript'' src=''{0}''></script>\n"
                    + "<script type=''text/javascript'' src=''{1}''></script>\n" + "<script type=''text/javascript''>\n"
                    + "$(document).ready(function() '{' $(''#myTable'').dataTable('{'\"bPaginate\": false, \"aaSorting\": [[3, \"asc\"]], \"bStateSave\": true'}') '}')\n"
                    + "</script>\n";

    private String jqueryUri;
    private String dataTablesUri;

    public QueryWizardResultResponse(String jqueryUri, String datatablesUri) {
        this.jqueryUri = jqueryUri;
        this.dataTablesUri = datatablesUri;

    }

    @XmlElement(name = "queryId")
    private String queryId = "";
    @XmlElement
    private BaseQueryResponse response = null;

    public void setResponse(BaseQueryResponse response) {
        this.response = response;
    }

    public void setQueryId(String queryId) {
        this.queryId = queryId;
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
        return MessageFormat.format(DATA_TABLES_TEMPLATE, jqueryUri, dataTablesUri);
    }

    private void putTableCell(StringBuilder builder, String cellValue) {
        builder.append("<td style=\"border:solid\">\n");
        builder.append(cellValue);
        builder.append("</td>\n");
    }

    @Override
    public String getMainContent() {
        StringBuilder builder = new StringBuilder();

        builder.append("<br/><br/>");
        builder.append("<H2>Query ID : " + queryId + "</H2><br/><br/>");
        if (response == null || !response.getHasResults()) {
            builder.append("<H2>There aren't anymore results</H2>");
            return builder.toString();
        }

        builder.append("<div>\n");
        builder.append("<div id=\"myTable_wrapper\" class=\"dataTables_wrapper no-footer\">\n");
        builder.append("<table id=\"myTable\" class=\"dataTable no-footer\" role=\"grid\" aria-describedby=\"myTable_info\">\n");

        if (response instanceof EventQueryResponseBase) {
            EventQueryResponseBase tempResponse = (EventQueryResponseBase) response;

            HashSet<String> fieldnameSet = buildTableColumnHeadings(builder, tempResponse);
            HashMap<String,String> fieldNameToValueMap = new HashMap<>();

            builder.append("<tbody>");

            String dataType = "";
            for (EventBase event : tempResponse.getEvents()) {
                dataType = event.getMetadata().getDataType();
                builder.append("<tr>");
                putTableCell(builder, dataType);
                for (Object field : event.getFields()) {
                    if (field instanceof FieldBase) {
                        FieldBase defaultField = (FieldBase) field;
                        fieldNameToValueMap.put(defaultField.getName(), defaultField.getValueString());
                    }
                }

                for (String fieldStr : fieldnameSet) {
                    String fieldValue = fieldNameToValueMap.get(fieldStr);
                    if (fieldValue != null && fieldValue.length() > 255)
                        putTableCell(builder, fieldValue.substring(0, 254));
                    else
                        putTableCell(builder, fieldValue == null ? "" : fieldValue);

                }

                fieldNameToValueMap.clear();
                builder.append("</tr>");
            }
        } else if (response instanceof EdgeQueryResponseBase) {
            EdgeQueryResponseBase tempResponse = (EdgeQueryResponseBase) response;
            builder.append("<thead><tr><th>Source</th><th>Sink</th><th>Edge Type</th><th>Activity Date</th><th>Edge Relationship</th></tr></thead>");
            builder.append("<tbody>");
            for (EdgeBase edge : tempResponse.getEdges()) {
                builder.append("<tr>");
                putTableCell(builder, edge.getSource());
                putTableCell(builder, edge.getSink());
                putTableCell(builder, edge.getEdgeType());
                putTableCell(builder, edge.getActivityDate());
                putTableCell(builder, edge.getEdgeRelationship());
                builder.append("</tr>");
            }
        } else if (response instanceof MetadataQueryResponseBase) {
            MetadataQueryResponseBase tempResponse = (MetadataQueryResponseBase) response;
            builder.append("<thead><tr><th>Field Name</th><th>Internal Field Name</th><th>Data Type</th><th>Last Updated</th><th>Index only</th></tr></thead>");
            builder.append("<tbody>");
            for (MetadataFieldBase field : ((List<MetadataFieldBase>) (tempResponse.getFields()))) {
                builder.append("<tr>");
                putTableCell(builder, field.getFieldName());
                putTableCell(builder, field.getInternalFieldName());
                putTableCell(builder, field.getDataType());
                putTableCell(builder, field.getLastUpdated());
                putTableCell(builder, field.isIndexOnly().toString());
                builder.append("</tr>");
            }
        } else if (response instanceof IndexStatsResponse) {
            IndexStatsResponse tempResponse = (IndexStatsResponse) response;
            builder.append("<thead><tr><th>Field Name</th><th>Observed</th><th>Selectivity</th><th>Unique</th></tr></thead>");
            builder.append("<tbody>");
            for (FieldStat stat : tempResponse.getFieldStats()) {
                builder.append("<tr>");
                putTableCell(builder, stat.field);
                putTableCell(builder, String.valueOf(stat.observed));
                putTableCell(builder, String.valueOf(stat.selectivity));
                putTableCell(builder, String.valueOf(stat.unique));
            }
        } else {
            throw new RuntimeException("Cannot handle a " + response.getClass().getSimpleName() + " response type");
        }

        builder.append("</tbody>");

        builder.append("</table><br/><br/>");
        builder.append("  <div class=\"dataTables_info\" id=\"myTable_info\" role=\"status\" aria-live=\"polite\"></div>\n");
        builder.append("</div>\n");
        builder.append("</div>");

        builder.append("<FORM id=\"queryform\" action=\"/DataWave/BasicQuery/" + queryId
                        + "/showQueryWizardResults\"  method=\"get\" target=\"_self\" enctype=\"application/x-www-form-urlencoded\">");
        builder.append("<center><input type=\"submit\" value=\"Next\" align=\"left\" width=\"50\" /></center>");

        builder.append("</FORM>");

        return builder.toString();
    }

    private HashSet<String> buildTableColumnHeadings(StringBuilder builder, EventQueryResponseBase tempResponse) {

        HashSet<String> fieldnameSet = new HashSet<>();
        builder.append("<thead><tr><th>DataType</th>");
        for (EventBase event : tempResponse.getEvents()) {
            for (Object field : event.getFields()) {
                if (field instanceof FieldBase) {
                    fieldnameSet.add(((FieldBase) field).getName());
                }
            }
        }

        for (String fieldname : fieldnameSet) {
            builder.append("<th>");
            builder.append(fieldname);
            builder.append("</th>");
        }

        builder.append("</tr></thead>");

        return fieldnameSet;
    }

}
