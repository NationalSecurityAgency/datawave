Ext.define('HUD.view.querydetail.PercentDetailsList',{
	extend: 'Ext.grid.Panel',
	alias: 'widget.percentdetailslist',
	height: 400,
	store: 'PercentDetail',
	columns: [
        { header: 'Data',         dataIndex: 'metric_type',     align: 'right'},
        { header: 'In Last Hour', dataIndex: 'last_hour_total', align: 'right'},
        { header: 'Percent (%)',  dataIndex: 'percent',         align: 'right', renderer: Ext.util.Format.numberRenderer('000.00')}
	],
	
	initComponent: function() {
		if (window.console && window.console.log) {		
			console.log('Initialized the HUD.view.querydetail.PercentDetailsList view');
		}
		this.callParent(arguments);
	}	
});