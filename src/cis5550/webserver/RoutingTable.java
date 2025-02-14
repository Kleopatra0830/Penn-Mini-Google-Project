package cis5550.webserver;

import java.util.*;

import cis5550.tools.Logger;

public class RoutingTable {
	private static final Logger logger = Logger.getLogger(Server.class);
	private Map<String, Route> routesMap;
	private Map<String, List<String>> methodsAndPatterns;
	
	// EC
	private Map<String, Map<String, Route>> routesMapWithHost;
	private Map<String, Map<String, List<String>>> methodsAndPatternsWithHost;
	
	
	
	public RoutingTable() {
		this.routesMap = new HashMap<>();
		this.methodsAndPatterns = new HashMap<>();
		// EC
		this.routesMapWithHost = new HashMap<>();
		this.methodsAndPatternsWithHost = new HashMap<>();
	}

	/**
	 * Adds a route to the routing table.
	 * @param method the HTTP methods (GET, PUT, POST)
	 * @param pathPattern the path pattern
	 * @param handler an instance of Route
	 */
	protected void addRoute(String method, String pathPattern, Route handler) {
		String key = method + " " + pathPattern;
//		routesMap.putIfAbsent(key, handler); // assume the route is unique for the same pattern (always use the first one)
		routesMap.put(key, handler); // assume the route is unique
		
		if (methodsAndPatterns.containsKey(method)) {
			methodsAndPatterns.get(method).add(pathPattern);
		} else {
			List<String> patterns = new ArrayList<>();
			patterns.add(pathPattern);
			methodsAndPatterns.put(method, patterns);
		}
	}
	
	/**
	 * Adds a route to the routing table with host implementation.
	 * @param method
	 * @param pathPattern
	 * @param handler
	 * @param hostStr
	 */
	protected void addRouteWithHost(String method, String pathPattern, Route handler, String hostStr) {
		String key = method + " " + pathPattern;
		
		Map<String, Route> localRoutesMap = routesMapWithHost.getOrDefault(hostStr, new HashMap<>());
		Map<String, List<String>> localMethodsAndPatterns = methodsAndPatternsWithHost.getOrDefault(hostStr, new HashMap<>());
		
		localRoutesMap.put(key, handler); // assume the route is unique
		
		if (localMethodsAndPatterns.containsKey(method)) {
			localMethodsAndPatterns.get(method).add(pathPattern);
		} else {
			List<String> patterns = new ArrayList<>();
			patterns.add(pathPattern);
			localMethodsAndPatterns.put(method, patterns);
		}
		
		if (!routesMapWithHost.containsKey(hostStr)) { // host appeared before
			
			routesMapWithHost.put(hostStr, localRoutesMap);
			methodsAndPatternsWithHost.put(hostStr, localMethodsAndPatterns);
		} 
	}
	
	/**
	 * Get a route from the routing table based on method and path pattern.
	 * @param method
	 * @param pathPattern
	 * @return Route handler (may be null TODO: error handling)
	 */
	protected Route getRoute(String method, String pathPattern) {
		String key = method + " " + pathPattern;
		return routesMap.get(key);
	}
	
	/** (EC)
	 * Gets route handler with given method, URL and hostStr.
	 * note that host must exist
	 * @param method
	 * @param url
	 * @param hostStr
	 * @return Route handler (may be null TODO: error handling)
	 */
	public Route getRouteWithHost(String method, String pathPattern, String hostStr) {
		String key = method + " " + pathPattern;
		try {
			return routesMapWithHost.get(hostStr).get(key);
		} catch (Exception e) {
			logger.error("404 Host Not Found");
		}
		return null;
	}
	
	
	/**
	 * Gets pathPatterns with a given method.
	 * @param method
	 * @return a list of path patterns.
	 */
	protected List<String> getPatterns(String method) {
		return methodsAndPatterns.getOrDefault(method, new ArrayList<>());
	}
	
	/** (EC)
	 * Gets pathPatterns with a given method and host.
	 * Note that host must exist.
	 * @param method
	 * @return a list of path patterns.
	 */
	protected List<String> getPatternsWithHost(String method, String hostStr) {
		try {
			return methodsAndPatternsWithHost.get(hostStr).getOrDefault(method, new ArrayList<>());
		} catch (Exception e) {
			logger.error("Host Not Found");
		}
		return new ArrayList<>();
	}
	
	/**
	 * Checks if a given host is exist or not.
	 * @param hostStr
	 * @return true if exists, false otherwise.
	 */
	protected boolean checkHostExists(String hostStr) {
		if (routesMapWithHost.containsKey(hostStr)) return true;
		else return false;
	}
	
}
