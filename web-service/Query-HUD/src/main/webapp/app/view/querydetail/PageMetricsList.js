Ext.define('HUD.view.querydetail.PageMetricsList',{
	extend: 'Ext.grid.Panel',
	alias: 'widget.pagedetailslist',
	store: 'PageMetrics',
	height: 400,
	viewConfig: {	
		markDirty: false
	},	
	features: [{
		ftype: 'summary',
	}],
	columns: [
        { header: 'Page Num',              dataIndex: 'pageNumber',                        summaryRenderer: function(value, summaryData, dataIndex) { return 'Total:'}},
 	    { header: 'Time Waiting for User', dataIndex: 'waitingTime',       align: 'right', renderer: Ext.util.Format.numberRenderer('000,000.00'), summaryType: 'sum'},
	    { header: 'Time Working',          dataIndex: 'workingTime',       align: 'right', renderer: Ext.util.Format.numberRenderer('000,000.00'), summaryType: 'sum'},
        { header: 'Call Time',             dataIndex: 'callTime',          align: 'right', renderer: Ext.util.Format.numberRenderer('000,000.00'), summaryType: 'sum'},
        { header: 'Return Time',           dataIndex: 'returnTime',        align: 'right', renderer: Ext.util.Format.numberRenderer('000,000.00'), summaryType: 'sum'},
        { header: 'Serialization Time',    dataIndex: 'serializationTime', align: 'right', renderer: Ext.util.Format.numberRenderer('000,000.00'), summaryType: 'sum'},
        { header: 'Bytes Written',         dataIndex: 'bytesWritten',      align: 'right', renderer: Ext.util.Format.numberRenderer('000,000'),    summaryType: 'sum'}
	],
		
	initComponent: function() {
		if (window.console && window.console.log) {	
			console.log('Initialized the HUD.view.querydetail.PageMetricsList view');
		}
		this.callParent(arguments);
	}
});