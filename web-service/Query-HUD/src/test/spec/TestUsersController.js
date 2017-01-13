describe("ExtJS HUD App Users Test Suite", function() {
	var console       = console || {log: function() {}};
	
	var usersView = null;
	var usersStore = null;
	var usersModel = null;
	var usersController = null;
	
	/** setup method to be called before each test case */
	beforeEach (function() {
		// this assumes controller has already been created 
		usersController = window.TestApplication.getController('Users');
		
		usersStore      = usersController.getStore('Users');
		usersView       = usersController.getView('user.Select');
		usersModel		= usersController.getModel('User');
	});
	
	afterEach (function() {
		usersView       = null;
		usersModel      = null;
		usersStore      = null;
		usersController = null;
		selectedUserId     = null;
	});

	/** Test Controller */
	it ('Users controller should be loaded', function() {	
		expect(usersController != null).toBeTruthy();
		expect(usersController).toBeDefined();
	});		

	/** Test Store */
	it ('Users store should be loaded', function() {
		expect(usersStore != null).toBeTruthy();
		expect(usersStore).toBeDefined();
		expect(usersStore.getCount()).toBe(0);		
	});

	/** Test Model */
	it ('Users model should be loaded', function() {
		expect(usersModel != null).toBeTruthy();
		expect(usersModel).toBeDefined();
	});

	/** Test View */
	it ('Users view should be loaded', function() {
		expect(usersView != null).toBeTruthy();
		expect(usersView).toBeDefined();
	});

	/** Test loading sample data */
	it ('Simulated logging in as a general user', function() {
		var selectedUserId = loadTestUserData(usersStore, SINGLE_USER_DATA);
		expect(usersStore.getCount()).toBe(1);
		expect(selectedUserId).toBe(USER_ID);
		
		expect(getDefaultUserId()).toBe(USER_ID);		
	});

	/** Test loading sample data */
	it ('Simulated logging in as an admin user', function() {
		var selectedUserId = loadTestUserData(usersStore, ADMIN_USER_DATA);
		expect(usersStore.getCount()).toBe(3);
		expect(selectedUserId).toBe(null);
		
		expect(getDefaultUserId()).toBe(null);	
	});
});
