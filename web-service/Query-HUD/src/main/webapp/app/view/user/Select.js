Ext.define('HUD.view.user.Select', {
	extend: 'Ext.panel.Panel',
	alias: 'widget.userselect',
	bodyPadding: 10,	
	title: 'Select User',
	items: [{
           	 xtype: 'combo',
           	 name: 'userid',
       	 	 itemId: 'selectUserId',
           	 displayField: 'userid',
           	 valueField: 'userid',
           	 store: 'Users',
           	 fieldLabel: 'User ID',
    }],
	
	initComponent: function() {
		if (window.console && window.console.log) {	
			console.log('Initialized the HUD.view.user.Select view');
		}
		this.callParent(arguments);
	},
	
});

