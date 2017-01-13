describe("ExtJS HUD Application QuerySummaryMetrics Test Suite", function() {
	var querySummaryMetricsView       = null;
	var querySummaryMetricsModel      = null;
	var querySummaryMetricsStore      = null;
	var querySummaryMetricsController = null;	
	
	/** setup method to be called before each test case */
	beforeEach (function() {
		createComponentTestArea();
		
		// this assumes controller has already been created 
		querySummaryMetricsController = window.TestApplication.getController('QuerySummaryMetrics');
		querySummaryMetricsStore      = querySummaryMetricsController.getStore('QuerySummaryMetrics');
		querySummaryMetricsModel      = querySummaryMetricsController.getModel('QuerySummaryMetrics');
		querySummaryMetricsView       = Ext.create("HUD.view.querysummary.List", {renderTo: "componentTestArea"});
//		querySummaryMetricsView       = querySummaryMetricsController.getView('querysummary.List');
	});
	
	/** cleanup method to be called after each test case */
	afterEach (function() {
		querySummaryMetricsView       = null;
		querySummaryMetricsModel      = null;
		querySummaryMetricsStore      = null;
		querySummaryMetricsController = null;
	});

	/** Test Controller */
	it ('QuerySummaryMetrics controller should be loaded', function() {	
		expect(querySummaryMetricsController != null).toBeTruthy();
		expect(querySummaryMetricsController).toBeDefined();
	});		

	/** Test Store */
	it ('QuerySummaryMetrics store should be loaded', function() {
		expect(querySummaryMetricsStore != null).toBeTruthy();
		expect(querySummaryMetricsStore).toBeDefined();
		expect(querySummaryMetricsStore.getCount()).toBe(0);				
	});

	/** Test Model */
	it ('QuerySummaryMetrics model should be loaded', function() {
		expect(querySummaryMetricsModel != null).toBeTruthy();
		expect(querySummaryMetricsModel).toBeDefined();
	});

	/** Test View */
	it ('QuerySummaryMetrics view should be loaded', function() {
		expect(querySummaryMetricsView != null).toBeTruthy();		
		expect(querySummaryMetricsView).toBeDefined();
	});
		
	/** Test loading data into percent details */
	it ('Loaded Query Summary Metrics Data', function() {
		// load test query summary metrics data
		loadTestQuerySummaryMetricsData(querySummaryMetricsStore, SUMMARY_ALL_DATA);

		expect(querySummaryMetricsStore.getCount()).toBe(4);
		expect(querySummaryMetricsStore.getAt(0).get('hours')).toBe(1);
		expect(querySummaryMetricsStore.getAt(1).get('hours')).toBe(6);
		expect(querySummaryMetricsStore.getAt(2).get('hours')).toBe(12);
		expect(querySummaryMetricsStore.getAt(3).get('hours')).toBe(24);
	});
});