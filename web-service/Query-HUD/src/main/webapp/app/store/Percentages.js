Ext.define('HUD.store.Percentages', {
	extend:   'Ext.data.Store',
	require: 'HUD.model.Query',
	model:   'HUD.model.Percentages',
	
	initComponent: function() {
		this.callParent(arguments);
	}	
});

