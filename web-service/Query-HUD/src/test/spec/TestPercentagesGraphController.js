describe("ExtJS HUD Application PercentagesGraph Test Suite", function() {
	var percentagesGraphView       = null;
	var percentagesGraphModel      = null;
	var percentagesGraphStore      = null;
	var percentagesGraphController = null;	
	
	/** setup method to be called before each test case */
	beforeEach (function() {
		createComponentTestArea();

		// this assumes controller has already been created 
		percentagesGraphController = window.TestApplication.getController('PercentagesGraph');
		percentagesGraphStore      = percentagesGraphController.getStore('Percentages');
		percentagesGraphModel      = percentagesGraphController.getModel('Percentages');
		percentagesGraphView       = Ext.create("HUD.view.querydetail.PercentagesGraph", {renderTo: "componentTestArea"});
//		percentagesGraphView       = percentagesGraphController.getView('PercentagesGraph');
	});
	
	/** cleanup method to be called after each test case */
	afterEach (function() {
		percentagesGraphView       = null;
		percentagesGraphModel      = null;
		percentagesGraphStore      = null;
		percentagesGraphController = null;
	});

	/** Test Controller */
	it ('PercentagesGraph controller should be loaded', function() {	
		expect(percentagesGraphController != null).toBeTruthy();
		expect(percentagesGraphController).toBeDefined();
	});		

	/** Test Store */
	it ('PercentagesGraph store should be loaded', function() {
		expect(percentagesGraphStore != null).toBeTruthy();
		expect(percentagesGraphStore).toBeDefined();
		expect(percentagesGraphStore.getCount()).toBe(0);				
	});

	/** Test Model */
	it ('PercentagesGraph model should be loaded', function() {
		expect(percentagesGraphModel != null).toBeTruthy();
		expect(percentagesGraphModel).toBeDefined();
	});

	/** Test View */
	it ('PercentagesGraph view should be loaded', function() {
		expect(percentagesGraphView != null).toBeTruthy();		
		expect(percentagesGraphView).toBeDefined();
	});
		
	/** Test loading data into percentages graph */
	it ('Test showQueryPercentagesGraph', function() {
		// load test queries data
		var queriesController = window.TestApplication.getController('Queries');
		var queriesStore      = queriesController.getStore('Queries');		
		loadTestQueriesData(queriesStore, SMALL_QUERY_DATA);

		var query = queriesStore.data.items[0];
		var updatedQuery = updateQueryPageTimes(query);
		
		var pivotController = window.TestApplication.getController('TimeMetricsPivot');
		var pivotStore      = pivotController.getStore('TimeMetricsPivot');
		var pivotModel      = pivotController.getModel('TimeMetricPivot');
		var pivotView       = Ext.create("HUD.view.querydetail.TimeMetricsPivot", {renderTo: "componentTestArea"});		
		
		var computedMetricsPivotData = computeQueryTimeMetricsPivot(pivotView, pivotModel, updatedQuery.pageMetrics());
		pivotStore.loadRawData(computedMetricsPivotData.data);	
		
		var percentagesGraphData = computePercentages(pivotStore.data);
		percentagesGraphStore.loadRecords(percentagesGraphData);
		
		expect(percentagesGraphStore.getCount()).toBe(2);
		expect(percentagesGraphStore.getAt(0).get("type")).toBe(pivotStore.getAt(0).get("type"));
		expect(percentagesGraphStore.getAt(0).get("total")).toBe(pivotStore.getAt(0).get("total"));
		expect(percentagesGraphStore.getAt(1).get("type")).toBe(pivotStore.getAt(1).get("type"));
		expect(percentagesGraphStore.getAt(1).get("total")).toBe(pivotStore.getAt(1).get("total"));
		
		// cleanup
		clearData(queriesStore);
		clearData(pivotStore);		
	});
});