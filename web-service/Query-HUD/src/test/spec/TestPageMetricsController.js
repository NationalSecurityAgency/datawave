describe("ExtJS HUD Application PageMetrics Test Suite", function() {
	var metricsView       = null;
	var metricsModel      = null;
	var metricsStore      = null;
	var metricsController = null;	
	
	/** setup method to be called before each test case */
	beforeEach (function() {
		createComponentTestArea();

		// this assumes controller has already been created 
		metricsController = window.TestApplication.getController('PageMetrics');
		metricsStore      = metricsController.getStore('PageMetrics');
		metricsModel      = metricsController.getModel('PageMetric');
		metricsView       = Ext.create("HUD.view.querydetail.PageMetricsList", {renderTo: "componentTestArea"});
//		metricsView       = metricsController.getView('PageMetricsList');
	});
	
	/** cleanup method to be called after each test case */
	afterEach (function() {
		metricsView       = null;
		metricsModel      = null;
		metricsStore      = null;
		metricsController = null;
	});

	/** Test Controller */
	it ('PageMetrics controller should be loaded', function() {	
		expect(metricsController != null).toBeTruthy();
		expect(metricsController).toBeDefined();
	});		

	/** Test Store */
	it ('PageMetrics store should be loaded', function() {
		expect(metricsStore != null).toBeTruthy();
		expect(metricsStore).toBeDefined();
		expect(metricsStore.getCount()).toBe(0);				
	});

	/** Test Model */
	it ('PageMetrics model should be loaded', function() {
		expect(metricsModel != null).toBeTruthy();
		expect(metricsModel).toBeDefined();
	});

	/** Test View */
	it ('PageMetrics view should be loaded', function() {
		expect(metricsView != null).toBeTruthy();		
		expect(metricsView).toBeDefined();
	});
		
	/** Test loading data into query page metrics */
	it ('Test showQueryPageMetrics', function() {
		// load test queries data
		var queriesController = window.TestApplication.getController('Queries');
		var queriesStore      = queriesController.getStore('Queries');		
		loadTestQueriesData(queriesStore, SMALL_QUERY_DATA);
		
		var query = queriesStore.data.items[0];
		var updatedQuery = updateQueryPageTimes(query);
		
		metricsStore.loadRecords(updatedQuery.pageMetrics().data.items);	
		
		expect(metricsStore.getCount()).toBe(updatedQuery.pageMetrics().getCount());
		
		// cleanup
		clearData(queriesStore);
	});
});