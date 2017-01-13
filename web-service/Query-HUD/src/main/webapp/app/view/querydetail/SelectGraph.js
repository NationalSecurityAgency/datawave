Ext.define('HUD.view.querydetail.SelectGraph',{
	extend: 'Ext.panel.Panel',
	alias:  'widget.selectgraph',
	height: 500,
	html:   "No Query Selected",
	// same data/store is used to display line chart and bar chart
	items: [{
		xtype: 'chart',
		alias: 'widget.linegraph',
		width: 750,
		height: 475,
		hidden: true,
   		style: 'background:#fff',
		store: 'PageMetrics',
		legend: {
			position: 'bottom'
		},
   		axes: [{
   			type: 'Numeric',
   			minimum: 0,
   			position: 'left',
   			fields: ['workingTime', 'waitingTime'],
   			title: 'Time (s)'
   		}, {
   			type: 'Category',
   			position: 'bottom',
   			fields: [ 'pageNumber'],
   			title: 'Page Number'        
  		} ],
   		series: [{
   			type: 'line',
   			axis: 'left',
   			xField: 'pageNumber',
   			yField: 'workingTime',
   			title:  'Time Working',
   			style: {
   				fill: '#00FF00'
   			}
   		}, {
   			type: 'line',
   			axis: 'left',
   			xField: 'pageNumber',
   			yField: 'waitingTime',
   			title:  'Time Waiting for User',
   			style: {
   				fill: '#FF0000'
   			}
  		}]	
	}, {
		xtype: 'chart',
		alias: 'widget.timedetailsbargraph',
		width: 750,
		height: 475,
		hidden: true,
    	style: 'background:#fff',
		store: 'PageMetrics',
		legend: {
			position: 'bottom'
		},
   		axes: [{
   			type: 'Numeric',
   			position: 'left',
   			fields: ['wait', 'work'],
   			label: {
   				renderer: Ext.util.Format.numberRenderer('0')
   			},
   			title: 'Time (s)',
   			grid: true,
   			minimum: 0
   		}, {
   			type: 'Category',
   			position: 'bottom',
   			fields: [ 'pageNumber'],
   			title: 'Page Number'        
  		} ],
  		series: [{
   			type: 'column',
   			stacked: true,
   			axis: 'left',
   			label: {
   				display: 'insideEnd',
   				'text-anchor': 'middle',
   				field: ['waitingTime', 'workingTime'],
   				renderer: Ext.util.Format.numberRenderer('0'),
   				orientation: 'vertical',
   			},
   			xField: 'pageNumber',
   			yField: ['waitingTime', 'workingTime'],
 			// want wait to be red, work to be green
   			getLegendColor: function(index) {
   				return ['rgb(255, 0, 0)', 'rgb(0,255, 0)'][index % 2];
   			},
   			renderer: function(sprite, record, attr, index, store) {
   				return Ext.apply(attr, {
					fill: ['rgb(255, 0, 0)', 'rgb(0,255, 0)'][index % 2]
    			});
    		},
    		title: ['Time Waiting for User', 'Time Working']
    	}]	
	} ],
	tbar: [ {
		text: "Line Chart",
		handler: function() {
			if (selectedQuery != null) {	
				graphPanel.html = "";	
				graphPanel.items.items[0].show();  // show line
				graphPanel.items.items[1].hide();  // hide bar
				graphPanel.doLayout();
			} else {
				graphPanel.html = "No Query Selected";
			}
 		}
	}, {
		text: "Bar Chart",
		handler: function() {
			if (selectedQuery != null) {		
				graphPanel.html = "";	
				graphPanel.items.items[0].hide();  // hide line
				graphPanel.items.items[1].show();  // show bar
				graphPanel.doLayout();
			} else {
				graphPanel.html = "No Query Selected";
			}
		}
	}],
	
	initComponent: function() {
		if (window.console && window.console.log) {	
			console.log('Initialized the HUD.view.querydetail.SelectGraph view');
		}
		 				
		this.callParent(arguments);
		
		graphPanel = this;
		
		// if query is selected, initially show line chart
		if (selectedQuery != null) {
			this.items.items[0].show();
			this.update("");
		}
	}	
});