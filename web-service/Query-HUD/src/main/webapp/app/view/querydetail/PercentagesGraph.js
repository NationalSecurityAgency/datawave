Ext.define('HUD.view.querydetail.PercentagesGraph',{
	extend: 'Ext.panel.Panel',
	alias:  'widget.percentagespanel',
	height: 400,
	html:   "No Query Selected",
	items: [{ 
			xtype: 'chart',
			alias: 'widget.percentagesgraph',
			width: 450,
			height: 300,
			hidden: true,
   			style: 'background:#fff',
    		store: 'Percentages',
			legend: {
				position: 'left'
			},
    		series: [{
       			type: 'pie',
       			field: 'total',
       			showInLegend: true,
       			donut: false,
       			tips: {
       				trackMouse: true,
       				width: 225,
       				height: 28,
       				renderer: function(storeItem, item) {
       					// calculate percentage
       					var total = 0;
       					// a panel doesn't have a store
						graphPanel.items.items[0].getStore().each(function(rec) {       					
       						total += rec.get('total');
       					});
       					// when mouseover slice, show: "Time Waiting for User: <time in s>: <pct of total>"
       					this.setTitle(storeItem.get('type') + ': ' + Ext.util.Format.number(storeItem.get('total'), '000.00') + ' (' + Ext.util.Format.number(storeItem.get('total') / total *100, '000.00') + '%)');
        			}
       			},
       			label: {
       				field: 'type',
       				display: 'rotate',
       				contrast: true,
       			},
       			getLegendColor: function(index) {
       				return ['rgb(255, 0, 0)', 'rgb(0,255, 0)'][index % 2];
       			},
       			renderer: function(sprite, record, attr, index, store) {
       				return Ext.apply(attr, {
						fill: ['rgb(255, 0, 0)', 'rgb(0,255, 0)'][index % 2]
       				});
       			}
    		}]		
	}],

	initComponent: function() {
		if (window.console && window.console.log) {	
			console.log('Initialized the HUD.view.querydetail.PercentagesGraph view');
		}
				
		this.callParent(arguments);
		graphPanel = this;
		
		// if query is selected, initially show pie chart
		if (selectedQuery != null) {
			this.items.items[0].show();
			this.update("");
		}		
	}	
});