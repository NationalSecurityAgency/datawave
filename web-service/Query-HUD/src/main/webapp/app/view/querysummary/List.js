Ext.define('HUD.view.querysummary.List',{
	extend: 'Ext.grid.Panel',
	alias: 'widget.metricsummarylist',
	store: 'QuerySummaryMetrics',
	columns: [
        { header: 'Hours',                    dataIndex: 'hours',                 flex:1, align: 'right'},
        { header: 'Query Count',              dataIndex: 'queryCount',            flex:1, align: 'right'},
        { header: 'Min Page Response Time',   dataIndex: 'minPageResponseTime',   flex:1, align: 'right'},
        { header: 'Max Page Response Time',   dataIndex: 'maxPageResponseTime',   flex:1, align: 'right'},
        { header: 'Total Page Response Time', dataIndex: 'totalPageResponseTime', flex:1, align: 'right'},
        { header: 'Total Pages',              dataIndex: 'totalPages',            flex:1, align: 'right'},
        { header: 'Min Page Size',            dataIndex: 'minPageResultSize',     flex:1, align: 'right'},
        { header: 'Max Page Size',            dataIndex: 'maxPageResultSize',     flex:1, align: 'right'},
        { header: 'Num Results',              dataIndex: 'totalPageResultSize',   flex:1, align: 'right'}
	],
	         
	initComponent: function() {
		if (window.console && window.console.log) {	
			console.log('Initialized the HUD.view.querysummary.List view');
		}
		this.callParent(arguments);
	}	
});