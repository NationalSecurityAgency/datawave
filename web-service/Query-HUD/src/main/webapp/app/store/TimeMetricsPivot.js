Ext.define('HUD.store.TimeMetricsPivot', {
	extend:  'Ext.data.Store',
	require: 'HUD.model.Query',
	model:   'HUD.model.TimeMetricPivot',
	
	initComponent: function() {
		this.callParent(arguments);
	}	
});

