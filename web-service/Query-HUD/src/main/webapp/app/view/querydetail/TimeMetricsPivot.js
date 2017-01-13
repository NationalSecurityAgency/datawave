Ext.define('HUD.view.querydetail.TimeMetricsPivot',{
	extend: 'Ext.grid.Panel',
	alias: 'widget.timedetailspivot',
	store: 'TimeMetricsPivot',
	columnLines: true,
	
	// bug in ext js, horizontal scrollbar doesn't work unless height is set
	// this is ok for this grid only since it only has 2 data rows (work and wait times)
	height: 100,
	// these are placeholder columns, these are generated when a user selects a query
	columns: [
	    { header: 'Type',  dataIndex: 'type'},
	    { header: 'Total', dataIndex: 'total'}
	],
	
	initComponent: function() {
		if (window.console && window.console.log) {	
			console.log('Initialized the HUD.view.querydetail.TimeMetricsPivot view');
		}
		this.callParent(arguments);
	}		
});