Ext.define('HUD.view.query.List',{
	extend: 'Ext.grid.Panel',
	alias: 'widget.querylist',
	title: 'Queries',
	store: 'Queries',
	height: 200,	
	columns: [
		{ header: 'Query ID',         dataIndex: 'id',             flex: 2},
		{ header: 'User ID',          dataIndex: 'userid',         flex: 1},
		{ header: 'System From',      dataIndex: 'systemFrom',     flex: 1},
		{ header: 'Query Name',       dataIndex: 'queryName',      flex: 2},
		{ header: 'Query Type',       dataIndex: 'queryLogicName', flex: 1},
		{ header: 'Create Date',      dataIndex: 'createDate',     flex: 2},
		{ header: 'Last Updated',     dataIndex: 'lastUpdated',    flex: 2},
		{ header: 'Num Pages',        dataIndex: 'numPages',       flex: 1, align: 'right', renderer: Ext.util.Format.numberRenderer('000,000')},
		{ header: 'Num Results',      dataIndex: 'numResults',     flex: 1, align: 'right', renderer: Ext.util.Format.numberRenderer('000,000')},
		{ header: 'Status',           dataIndex: 'lifeCycle',      flex: 1},
	],
	         
	initComponent: function() {
		if (window.console && window.console.log) {	
			console.log('Initialized the HUD.view.query.List view');
		}
		this.callParent(arguments);
	}		 
});
