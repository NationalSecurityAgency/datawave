describe("ExtJS HUD Application TimeMetricsPivot Test Suite", function() {
	var pivotView       = null;
	var pivotModel      = null;
	var pivotStore      = null;
	var pivotController = null;	
	
	/** setup method to be called before each test case */
	beforeEach (function() {
		createComponentTestArea();
			
		// this assumes controller has already been created 
		pivotController = window.TestApplication.getController('TimeMetricsPivot');
		pivotStore      = pivotController.getStore('TimeMetricsPivot');
		pivotModel      = pivotController.getModel('TimeMetricPivot');
		pivotView       = Ext.create("HUD.view.querydetail.TimeMetricsPivot", {renderTo: "componentTestArea"});
	});
	
	/** cleanup method to be called after each test case */
	afterEach (function() {
		pivotView       = null;
		pivotModel      = null;
		pivotStore      = null;
		pivotController = null;
	});

	/** Test Controller */
	it ('TimeMetricsPivot controller should be loaded', function() {	
		expect(pivotController != null).toBeTruthy();
		expect(pivotController).toBeDefined();
	});		

	/** Test Store */
	it ('TimeMetricsPivot store should be loaded', function() {
		expect(pivotStore != null).toBeTruthy();
		expect(pivotStore).toBeDefined();
		expect(pivotStore.getCount()).toBe(0);				
	});

	/** Test Model */
	it ('TimeMetricsPivot model should be loaded', function() {
		expect(pivotModel != null).toBeTruthy();
		expect(pivotModel).toBeDefined();
	});

	/** Test View */
	it ('TimeMetricsPivot view should be loaded', function() {
		expect(pivotView != null).toBeTruthy();		
		expect(pivotView).toBeDefined();
	});
		
	/** Test creating pivot table from page metrics */
	it ('Test computeQueryTimeMetricsPivot', function() {
		// load test queries data
		var queriesController = window.TestApplication.getController('Queries');
		var queriesStore      = queriesController.getStore('Queries');		
		loadTestQueriesData(queriesStore, SMALL_QUERY_DATA);
		
		var query = queriesStore.data.items[0];
		var updatedQuery = updateQueryPageTimes(query);

		var computedMetricsPivotData = computeQueryTimeMetricsPivot(pivotView, pivotModel, updatedQuery.pageMetrics());
		expect(computedMetricsPivotData.data.length).toBe(2);	
		
		pivotStore.loadRawData(computedMetricsPivotData.data);	
		expect(pivotStore.getCount()).toBe(2);
		
		var fields = pivotModel.getFields();
		expect(fields.length).toBe(2 + updatedQuery.pageMetrics().getCount());  // type, total, pages 1-N		
		
		// cleanup
		clearData(queriesStore);		
	});
});