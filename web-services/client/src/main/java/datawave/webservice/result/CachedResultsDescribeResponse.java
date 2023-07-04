package datawave.webservice.result;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

@XmlRootElement(name = "CachedResultsDescribeResponse")
@XmlType(propOrder = {"view", "numRows", "columns"})
@XmlAccessorType(XmlAccessType.NONE)
public class CachedResultsDescribeResponse extends BaseResponse {

    private static final long serialVersionUID = 1L;

    @XmlElement(name = "View")
    private String view = null;

    @XmlElementWrapper(name = "Columns")
    @XmlElement(name = "column")
    private List<String> columns = new ArrayList<String>();

    @XmlElement(name = "NumRows")
    private Integer numRows = null;

    public String getView() {
        return view;
    }

    public void setView(String view) {
        this.view = view;
    }

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public Integer getNumRows() {
        return numRows;
    }

    public void setNumRows(Integer numRows) {
        this.numRows = numRows;
    }
};
