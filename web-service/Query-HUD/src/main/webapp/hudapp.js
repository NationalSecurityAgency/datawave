Ext.define('Datawave.QueryHudConstants', {
    runningQueriesRefreshInterval: 1000 * 60 * 0.5, 
    summaryQuerMetricRefreshInterval: 1000 * 60 * 2
});


Ext.override(Ext.data.Connection, {
    withCredentials: true, //make sure we always include the client certificate
    method: 'GET'//default ajax requests to GET
});

var app           = null;
var selectedUserId   = null;   // string containing userid
var selectedQuery = null;   // HUD.model.Query object
var newUser       = false;

Ext.application({
	requires: ['Ext.container.Viewport'],
	name: 'HUD',
	appFolder: 'app',
	
	controllers: ['Users', 'Queries', 'PageMetrics', 'TimeMetricsPivot', 'SelectGraph', 'PercentagesGraph', 'PercentDetails', 'QuerySummaryMetrics'],
	
	launch: function() {
		Ext.create('Ext.panel.Panel', {
			renderTo: Ext.getBody(),
			title: 'Query HUD',
	        scrollable: {
	            direction: 'both'
	        },
			items: [
			        {
			        	xtype: 'userselect',
			        	title: "Select User"
			        },
			        {
			        	xtype: 'querylist',
			        	title: "Running Queries"
			        },
			        {
			        	xtype: "tabpanel",
			        	activeTab: 0,
			        	title: "Query Details: No Query Selected",
			        	cls: "details-container",
			        	items: [
			        	        {
			        	        	xtype: 'pagedetailslist',
			        	        	title: "Metrics and Times by Page"
			        	        },
			        	        {
			        	        	xtype: 'timedetailspivot',
			        	        	title: "Pages by Working/Waiting Time"
			        	        },
			        	        {
			        	        	xtype: 'selectgraph',
			        	        	title: "Pages by Working/Waiting Times Graph",
			        	        },
			        	        {
			        	        	xtype: 'percentagespanel',
			        	        	title: "Percentages of Working/Waiting Times Graph"
			        	        },
			        	        {
			        	        	xtype: 'percentdetailslist',
			        	        	title: "Percentages",
			        	        }
			        	        ]
			        },			        	        
			        {
			        	xtype: 'metricsummarylist',
			        	title: "Summary Query Metrics"
			        }
			]
		});
		
  		app = HUD.getApplication();  
  		window.Application = HUD.getApplication();  
				
        var runningQueriesRefresh = function() {
           	if (selectedUserId != null) {
	        	var querylist = Ext.ComponentQuery.query('panel[alias=widget.querylist]')[0];
	        	var queryStore = querylist.getStore();
	        	queryStore.load({
	        		id: selectedUserId,
	        		callback: function(records, operation, success) {
	       				if (newUser && selectedQuery == null) {
       						var query = getDefaultQuery();
       						if (query != null) {
       							selectedQuery = query;
       							selectQuery(selectedQuery);
       						}
	       				}
	            			
	           			if (selectedQuery != null) {
	           				var queryId = selectedQuery.get("id");
	           				for (var i = 0; i < records.length; i++) {
	           					var record = records[i];
	           					var recordId = record.get("id");
	           					if (recordId == queryId) {
	           						selectedQuery = record;
	           						
	           						record = updateQueryPageTimes(selectedQuery);
	           						records[i] = record;
	           						
	           						showQueryPageMetrics(selectedQuery);
	           						showQueryTimeMetricsPivot(selectedQuery);
	           						showQueryTimeGraph(selectedQuery);
	           						showQueryPercentagesGraph(selectedQuery);
	           						showQueryPercentages(selectedQuery);
	           					}
	           				}
	           			}
	           		}
	           	});
            }
         };
        
        // load users and select default user, if any
   		var usersController = app.getController('Users', false);  
    	var usersStore = usersController.getStore('Users');		
    	usersStore.load({
    		callback: function(records, operation, success) {
    			newUser = false;
               	if (selectedUserId == null) {
			        // if only 1 user, select that user by default
       				selectedUserId = getDefaultUserId();
       				if (selectedUserId != null) {
       					selectUserId(selectedUserId);
       					newUser = true;
       					runningQueriesRefresh();
       				}
               	}
            }
       	});
       	
        var queryRefreshTask = {
        	run: runningQueriesRefresh,
            fireOnStart: true,
            interval: 30000
        };
		Ext.TaskManager.start(queryRefreshTask);
		
		var summaryQueryMetricRefresh = {
            run: function() {
           		var summarylist = Ext.ComponentQuery.query('panel[alias=widget.metricsummarylist]')[0];
           		var summaryStore = summarylist.getStore();
           		summaryStore.load();
            },
            interval: 60000
        };
        
        Ext.TaskManager.start(summaryQueryMetricRefresh);
		        
	},
	useridSelected: function(userid) {		
		selectedUserId = userid;
		var queryList = Ext.ComponentQuery.query('panel[alias=widget.querylist]')[0];
		var queryStore = queryList.getStore();
		queryStore.load( {
			id: userid,
			callback: function(records, operation, success) {
			}
		});
	},
	querySelected: function(query) {
		selectedQuery = query;
		var detailsParentPanel = Ext.ComponentQuery.query('panel[cls=details-container]')[0];
		detailsParentPanel.setTitle("Query Details: " + selectedQuery.get('id'));
		
		selectedQuery = updateQueryPageTimes(selectedQuery);
		
		showQueryPageMetrics(selectedQuery);
		showQueryTimeMetricsPivot(selectedQuery);
		showQueryTimeGraph(selectedQuery);
		showQueryPercentagesGraph(selectedQuery);
		showQueryPercentages(selectedQuery);
	},
	
});


/**
 * Compute workingTime and waitintTime by page for selected query in seconds
 *
 * parameters:
 * selectedQuery - HUD.model.Query object
 *
 * return:
 * HUD.model.Query object with updated wait and work times
 *
 */
function updateQueryPageTimes(selectedQuery) {
    var prevReturnedRaw = 0;
        
	selectedQuery.pageMetrics().each(function (pageMetric) {		
        var pageNum = pageMetric.get("pageNumber");
        var waitTime = 0;

	    if (prevReturnedRaw > 0) {
	    	// time spent waiting	    	
	        var pageRequestedRaw = prevReturnedRaw;				
		    var pageReturnedRaw = pageMetric.get("pageRequested");	    
	        waitTime = (pageReturnedRaw-pageRequestedRaw)/1000;	    
	    }
	    pageMetric.set("waitingTime", waitTime);

	    var pageRequestedRaw = pageMetric.get("pageRequested");
				
		var pageReturnedRaw = pageMetric.get("pageReturned");
		prevReturnedRaw = pageReturnedRaw;	    	    
	    var workTime = (pageReturnedRaw-pageRequestedRaw)/1000;
	    
	    pageMetric.set("workingTime", workTime);
	    
		// convert times from ms to s
		var value = pageMetric.get("callTime")/1000;
		pageMetric.set("callTime", value);
			
		var value = pageMetric.get("returnTime")/1000;
		pageMetric.set("returnTime", value);
			
		var value = pageMetric.get("serializationTime")/1000;
		pageMetric.set("serializationTime", value);	    
	});	

	return selectedQuery;	
}


/**
 * show metrics and working/waiting times by page
 */
function showQueryPageMetrics(query) {
	var pageMetrics = query.pageMetrics();
	var pageDetailsList = Ext.ComponentQuery.query('pagedetailslist')[0];
    var pageDetailStore = pageDetailsList.getStore();
	pageDetailStore.loadRecords(pageMetrics.data.items);	
}


/**
 * create a "pivot" table for pages by workingTime and waitingTime
 */
function showQueryTimeMetricsPivot(query) {
	// create "pivot" table (convert rows to columns)
	var timeDetailsPivotList = Ext.ComponentQuery.query('panel[alias=widget.timedetailspivot]')[0];
	var timeDetailsPivotStore = timeDetailsPivotList.getStore();	
	var computedMetricsPivotData = computeQueryTimeMetricsPivot(timeDetailsPivotList, timeDetailsPivotStore.model, query.pageMetrics());
	timeDetailsPivotStore.loadRawData(computedMetricsPivotData.data);	
}


/**
 * show line chart if both charts are hidden
 */
function showQueryTimeGraph(query) {
	var selectGraphPanel = Ext.ComponentQuery.query('panel[alias=widget.selectgraph]')[0];
	if (selectGraphPanel.items.items[0].isHidden() && selectGraphPanel.items.items[1].isHidden()) {
		selectGraphPanel.items.items[0].show(); 
	};	
	selectGraphPanel.update("");
}

/**
 * compute data for pie chart, total of work and wait time for all pages
 * ASSUMES: pivot data was already computed 
 */
function showQueryPercentagesGraph(query) {
	// pivot table data
	var timeDetailsPivotList = Ext.ComponentQuery.query('panel[alias=widget.timedetailspivot]')[0];
	var timeDetailsPivotStore = timeDetailsPivotList.getStore();	

	// create percentages 
	var barGraphPanel = Ext.ComponentQuery.query('panel[alias=widget.percentagespanel]')[0];
	var percentagesGraph = barGraphPanel.items.items[0];
	var percentagesStore = percentagesGraph.getStore();	
	var percentagesGraphData = computePercentages(timeDetailsPivotStore.data);
	percentagesStore.loadRecords(percentagesGraphData);

	// show graph if hidden	
	if (barGraphPanel.items.items[0].isHidden()) {
		barGraphPanel.items.items[0].show();
	};	
	barGraphPanel.update("");
}


/**
 * compute data for query pages, results, and bytes for last hour
 */
function showQueryPercentages(query) {
	var metricsSummaryList = Ext.ComponentQuery.query('panel[alias=widget.metricsummarylist]')[0];
	var metricsSummaryStore = metricsSummaryList.getStore();	
	var computedPercentagesData = computeQueryMetricsPercentages(query.pageMetrics(), metricsSummaryStore);
	
	var percentDetailsList = Ext.ComponentQuery.query('panel[alias=widget.percentdetailslist]')[0];
	var percentDetailsStore = percentDetailsList.getStore();	
	percentDetailsStore.loadRecords(computedPercentagesData);	
}


/**
 * converts data from pageMetrics.data.items to JSON data, dynamically making rows "columns"
 * NOTE that the data is returned as JSON, not sure if there is a way to dynamically create an object's columns 
 *
 * parameters:
 * view      - HUD.view.querydetail.TimeMetricsPivot object
 * model     - HUD.model.TimeMetricPivot object
 * dataItems - array of HUD.model.PageMetric objects
 *
 * return:
 * array of HUD.model.TimeMetricPivot objects in JSON format
 */
function computeQueryTimeMetricsPivot(view, model, pageMetrics) {
	var rows = [];

    var waitData = [];
    var waitTotal = 0;
    var workData = [];
    var workTotal = 0;   
    
    // compute wait and work total times
    var p=0;
	pageMetrics.each(function (pageMetric) {	
        var pageNum = pageMetric.get("pageNumber");
        var workTime = pageMetric.get("workingTime");
        var waitTime = pageMetric.get("waitingTime");

		waitData[p] = waitTime;
		waitTotal = waitTotal + waitTime;
		workData[p] = workTime;
		workTotal = workTotal + workTime;
		
		p++;	
	});
    	
	// add fields to TimeMetricPivot model object
	var fields = [];
	var field = Ext.create('Ext.data.Field', {
		name: 'type'
	});
	fields.push(field);
	
	for (var i = 0; i < p; i++) {
		field = Ext.create('Ext.data.Field', {
			name: 'page' + (i+1)
		});
		fields.push(field);		
	}
	field = Ext.create('Ext.data.Field', {
		name: 'total'
	});
	fields.push(field);
	model.setFields(fields);

	// add columns to TimeMetricPivot view object
	var columns = [];
	var column = null;
	// TODO - make the first column locked like Excel
	for (var i=0; i < fields.length; i++) {
		if (i == 0) {
			column = {header: capitalize(fields[i].name), dataIndex: fields[i].name};
		} else {
			column = {header: capitalize(fields[i].name), dataIndex: fields[i].name, align: 'right', renderer: Ext.util.Format.numberRenderer('000,000.00')};
		}
		columns.push(column);
	}
	view.reconfigure(null, columns);
	
	// build JSON data
    var jsonData = '{"data": [';

    // create row for wait data
    var rowCfg = '{"type": "Time Waiting for User",';
    for (var i=1; i < fields.length-1; i++) {
    	rowCfg = rowCfg + '"' + fields[i].name+ '":' + waitData[i-1] + ',';
    }
    rowCfg = rowCfg + '"total": ' + waitTotal + '}';
	jsonData = jsonData + rowCfg;

	// create row for work data
    rowCfg = '{"type": "Time Working",';
    for (var i=1; i < fields.length-1; i++) {
    	rowCfg = rowCfg + '"' + fields[i].name+ '":' + workData[i-1] + ',';
    }
    rowCfg = rowCfg + '"total": ' + workTotal + '}';
	jsonData = jsonData + ',' + rowCfg;
    
    jsonData = jsonData + ']}';
    return Ext.decode(jsonData);
};


/**
 * Creates summary of total times by type (working, waiting for user)
 * 
 * parameters:
 * dataItems - array of HUD.model.TimeMetricPivot objects
 *
 * return:
 * array of HUD.model.Percentages objects 
 */
function computePercentages(dataItems) {
	var rows = [];
    
	dataItems.each(function (data) {
		var row = Ext.create('HUD.model.Percentages', {
    				type : data.get('type'),
    				total : data.get('total')
		});
		rows.push(row);
    });
	
	return rows;
}


/**
 * converts data from pageMetrics.data.items to add rows for calculating percentages
 * 
 * parameters:
 * pageMetrics - list of HUD.model.PageMetric objects
 * metricsSummaryStore - HUD.store.QuerySummaryMetrics, summary metrics data
 *
 * return:
 * array of HUD.model.PercentDetails objects
 */
function computeQueryMetricsPercentages(pageMetrics, metricsSummaryStore) {
	var rows = [];
    
	var oneHourMetrics = metricsSummaryStore.getById(1);
	var msInOneHour = 1000 * 60 * 60;
	var now = new Date().getTime();
	var pageMetricsUnder1Hour = new Array();
	var numUnder1Hour = 0;
	pageMetrics.each(function (pageMetric) {
		var pageRequested = pageMetric.get('pageRequested');
		if(now - pageRequested < msInOneHour) {
			pageMetricsUnder1Hour[numUnder1Hour] = pageMetric;
			numUnder1Hour = numUnder1Hour + 1;
		}
	});
	
	var bytesInOneHr = 0;
	var resultsInOneHr = 0;
	var pagesInOneHr = 0;
	
	for(var i = 0; i < pageMetricsUnder1Hour.length; i++) {
		bytesInOneHr = bytesInOneHr + pageMetricsUnder1Hour[i].get('bytesWritten');
		resultsInOneHr = resultsInOneHr + pageMetricsUnder1Hour[i].get('pagesize');
		pagesInOneHr = pagesInOneHr + 1;
	}
	
	var percentPages = 0;
	var totalPagesInOneHr = oneHourMetrics.get("totalPages");
	if(totalPagesInOneHr != 0) {
		percentPages = ((pagesInOneHr * 1.0) / (totalPagesInOneHr * 1.0)) * 100.0;
	}
		
	var percentResults = 0;
	var totalResultsInOneHr = oneHourMetrics.get("totalPageResultSize");
	if(totalPagesInOneHr != 0) {
		percentResults = ((resultsInOneHr * 1.0) / (totalResultsInOneHr * 1.0)) * 100.0;
	}
	    
	var row = Ext.create('HUD.model.PercentDetails', {
    			metric_type : 'Pages:',
    			last_hour_total : pagesInOneHr,
    			percent : (percentPages).toFixed(2)
	});
	rows.push(row);

	row = Ext.create('HUD.model.PercentDetails', {
    			metric_type : 'Results:',
    			last_hour_total : resultsInOneHr,
    			percent : (percentResults).toFixed(2)
	});
	rows.push(row);

	row = Ext.create('HUD.model.PercentDetails', {
    			metric_type : 'Bytes:',
    			last_hour_total : bytesInOneHr,
    			percent : 0
	});
	rows.push(row);
	
	return rows;
};

	
/**
 * changes the first letter to upper case, leaving the rest of the string untouched
 *
 * parameters:
 * data - string to initial upper case
 *
 * return:
 * string with first letter in upper case
 */
function capitalize(data) {
	return data.charAt(0).toUpperCase() + data.slice(1);
}


/**
 * determine if a userid should be selected
 * if there is only userid in the user's list, return that one
 * otherwise, return null
 * 
 * return:
 * userid - userid or null
 */
function getDefaultUserId() {
	var defaultUserId = null;
	
   	var usersController = app.getController('Users', false);  
    var usersStore = usersController.getStore('Users');			

    if (usersStore.data.items.length == 1) {
    	var record = usersStore.data.items[0];
    	defaultUserId = record.get("userid");
    }	
	
	return defaultUserId;
}

/**
 * make specified userid selected in picklist
 * 
 * parameters:
 * userid - userid to select
 */
function selectUserId(userid) {
	var selectUser = Ext.ComponentQuery.query('combo[itemId=selectUserId]')[0];
	selectUser.select(userid);
}


/**
 * determine if a query should be selected
 * if there is only one query in the users query list OR only query with a non CLOSED status, return that record
 * otherwise, return null
 * 
 * return:
 * query - query or null
 */
function getDefaultQuery() {
	var defaultQuery = null;
	
   	var queriesController = app.getController('Queries', false);  
    var queriesStore = queriesController.getStore('Queries');			
	
    if (queriesStore.data.items.length == 1) {
    	var record = queriesStore.data.items[0];
    	defaultQuery = record;
    } else {
    	var tempRecord = null;
    	var nonClosedCnt = 0;
    	for (var i=0; i < queriesStore.data.items.length; i++) {
    		var record = queriesStore.data.items[i];
    		var status = record.get("lifeCycle");
   			if (record.get("lifeCycle") != "CLOSED") {
    			nonClosedCnt = nonClosedCnt + 1;
    			tempRecord = record;
    		}
    	}
    	if (nonClosedCnt == 1) {
    		defaultQuery = tempRecord;
    	}
    }
	
	return defaultQuery;
}


/**
 * make specified query selected/highlighted
 * note that it looks up the row index, 
 *      since the user can sort the data or add/deleted queries, 
 *      which means the index can change between refreshes
 */
function selectQuery(query) {
	var userQueryList = Ext.ComponentQuery.query('panel[alias=widget.querylist]')[0];
	
	// get row index
	var idx = null;
	var queriesStore = userQueryList.getStore();
    for (var i=0; i < queriesStore.data.items.length; i++) {
    	var record = queriesStore.data.items[i];
    	if (record.get("id") == query.get("id")) {
    		idx = i;
    	}
    }
	
	// highlight selected query
	userQueryList.getView().select(idx);
}
