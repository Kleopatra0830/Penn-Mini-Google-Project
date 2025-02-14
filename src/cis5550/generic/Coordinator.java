package cis5550.generic;

import java.util.*;
import cis5550.tools.*;
import static cis5550.webserver.Server.*;
//import cis5550.webserver.Server;

public class Coordinator {
//	private static final Logger logger = Logger.getLogger(Server.class);
	private static Map<String, WorkerEntry> activeWorkers = new HashMap<>();
	private static boolean afterInitFlag = false;
	
	private static class WorkerEntry {
		private String ip;
		private int port;
		private long lastPingTime;
		private static final int DEFAULT_MAX_INACTIVE_INTERVAL = 15;
		
		public WorkerEntry(String ip, int port) {
			this.ip = ip;
			this.port = port;
			this.lastPingTime = System.currentTimeMillis();
		}

		/**
		 * @return the ip
		 */
		public String getIp() {
			return ip;
		}

		/**
		 * @return the port
		 */
		public int getPort() {
			return port;
		}

		/**
		 * @param ip the ip to set
		 */
		public void setIp(String ip) {
			this.ip = ip;
		}

		/**
		 * @param port the port to set
		 */
		public void setPort(int port) {
			this.port = port;
		}
		
		@Override
		public String toString() {
			return ip + ":" + port;
		}
		
		public String generateHyperLink() {
			return "http://" + this.toString() + "/";
		}
		
		/**
		 * Checks if the worker entry is expired.
		 * @return true if expired, false otherwise.
		 */
		public boolean isExpired() {
			if (System.currentTimeMillis() - lastPingTime >= DEFAULT_MAX_INACTIVE_INTERVAL * 1000) {
				return true;
			} else {
				return false;
			}
		}
	}
	
	// methods
	/**
	 * Returns the current list of workers as ip:port strings.
	 * @return
	 */
	public static Vector<String> getWorkers() { 
		expireEntries();
		Vector<String> workersList = new Vector<>();

		for (Map.Entry<String, WorkerEntry> entry : activeWorkers.entrySet()) {
			WorkerEntry workerEntry = entry.getValue();
			if (!workerEntry.isExpired()) {
				workersList.add(workerEntry.toString());
			}
		}
		
		return workersList;
	}
//	protected static List<String> getWorkers() { 
//		expireEntries();
//		List<String> workersList = new ArrayList<>();
//		
//		for (Map.Entry<String, WorkerEntry> entry : activeWorkers.entrySet()) {
//			WorkerEntry workerEntry = entry.getValue();
//			if (!workerEntry.isExpired()) {
//				workersList.add(workerEntry.toString());
//			}
//		}
//		
//		return workersList;
//	}
	
	/**
	 * Returns the HTML table with the list of workers. ("/" route)
	 * @return
	 */
	protected static String workerTable() { // TODO
		StringBuilder htmlSb = new StringBuilder();
		
		htmlSb.append("<html><head><title>KVS Coordinator</title></head><body>");
		htmlSb.append("<h1>Active Workers</h1>");
		htmlSb.append("<table border=\"1\"><tr><th>ID</th><th>IP</th><th>Port</th></tr>");
		
		for (Map.Entry<String, WorkerEntry> entry : activeWorkers.entrySet()) {
			String id = entry.getKey();
			WorkerEntry workerEntry = entry.getValue();
			htmlSb.append("<tr><td><a href=" + workerEntry.generateHyperLink() + ">");
			htmlSb.append(id + "</a></td><td>" + workerEntry.getIp() + "</td><td>" + workerEntry.getPort() + "</td></tr>");
		}
		
		htmlSb.append("</table></body></html>");
		
		return htmlSb.toString();
	}
	
	/**
	 * Creates routes for the "/ping" and "/workers".
	 */
	protected static void registerRoutes() {
		// create /ping route
		get("/ping", (req, res) -> {
			String id = req.queryParams("id");
			int port = -1;
			try {
				port = Integer.parseInt(req.queryParams("port"));
			} catch (NumberFormatException e) {
				res.status(400, "Bad Request");
				return null;
			}
			if (id == null || port == -1) { // id or port is missing
				res.status(400, "Bad Request");
				return null;
			}
			String ip = req.ip();
			// store worker's info
			activeWorkers.put(id, new WorkerEntry(ip, port));
			if (activeWorkers.size() > 0 && !afterInitFlag) {
				System.out.println("Ready to go!");
				afterInitFlag = true;
			}
//			System.out.println(activeWorkers.size());
			res.status(200, "OK");
			return "OK";
		});
		// create /workers route
		get("/workers", (req, res) -> {
//			int k = activeWorkers.size();
			int k = 0;
			StringBuilder responseSb = new StringBuilder();
//			responseSb.append(k + "\n");
			
			for (Map.Entry<String, WorkerEntry> entry : activeWorkers.entrySet()) {
				String id = entry.getKey();
				WorkerEntry workerEntry = entry.getValue();
				if (!workerEntry.isExpired()) {
					responseSb.append(id + "," + workerEntry.toString() + "\n");
					k++;
				} 
			}
			res.status(200, "OK");
			
			return k + "\n" + responseSb.toString();
		});
	}
	
	/**
	 * Expire worker entries check. Removes the expired worker entries.
	 */
	protected static void expireEntries() {
//        for (Map.Entry<String, WorkerEntry> entry : activeWorkers.entrySet()) {
//            String id = entry.getKey();
//            WorkerEntry workerEntry = entry.getValue();
//
//            // Check if the session has expired
//            if (workerEntry.isExpired()) {
////            	System.out.println(activeWorkers.size());
//                // Session has expired; remove it
//                deleteEntry(id);
//            }
//        }
		// use iterator instead of for loop to avoid ConcurrentModificationException exception
		Iterator<Map.Entry<String, WorkerEntry>> iterator = activeWorkers.entrySet().iterator();
		while (iterator.hasNext()) {
		    Map.Entry<String, WorkerEntry> entry = iterator.next();
		    String id = entry.getKey();
		    WorkerEntry workerEntry = entry.getValue();

		    // Check if the session has expired
		    if (workerEntry.isExpired()) {
		        // Session has expired; remove it using the iterator
		        iterator.remove();
//		        logger.info("Remove a worker entry: " + id + "," + workerEntry.toString());
		    }
		}
	}

//	/**
//	 * Deletes an entry from activeWorkers.
//	 * @param id
//	 */
//	private static void deleteEntry(String id) {
//		if (activeWorkers.containsKey(id)) {
//			activeWorkers.remove(id);
//		}
//	}
}
