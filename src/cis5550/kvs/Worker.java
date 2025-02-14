package cis5550.kvs;

import static cis5550.webserver.Server.*;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import cis5550.kvs.KVSClient.WorkerEntry;
import cis5550.tools.HTTP;
import cis5550.tools.Logger;
import cis5550.webserver.Response;

public class Worker extends cis5550.generic.Worker { // TODO ensure thread safety
	// fields
	private static int workerPortNo;
	private static String storagePath;
	private static String coordinatorEntry;
	private static String workerId;
	private static KVSClient kvsClient;
	private static ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
//	private static Map<String, Map<String, List<Row>>> database = new ConcurrentHashMap<>();
	private final static Logger logger = Logger.getLogger(Worker.class);
	
	
	public static void main(String[] args) { // accepts three command-line arguments
		// initialize part
		init(args);
		// call startPingThread
		startPingThread(coordinatorEntry, workerPortNo, workerId, storagePath);
		// initialization for replica
		initialization();
		// get parameters and store data into the database
		getParamsAndPutData();
		// get parameters and get data from the database
		getParamasAndGetData();
		// get list of tables with counts of rows TODO (hyperlink)
		getTablesMetaData();
		// add the table viewer
		getTableView();
		// read the whole row
		getWholeRow();
		// read stream
		readStream();
		// get row count
		getRowCount();
		// get list of tables
		getTableList();
		// rename table
		renameTable();
		// delete table
		deleteTable();
		// handle put row request
		handlePutRow();
	}

	/**
	 * Initialization for replication purpose.
	 */
	private static void initialization() {
		// update worker list (EC)
	    scheduler.scheduleAtFixedRate(() -> {
			try {
				kvsClient.downloadWorkers();
			} catch (IOException e) {
//						e.getMessage();
//						e.printStackTrace();
			}
		}, 0, 5, TimeUnit.SECONDS);
	    scheduler.scheduleAtFixedRate(() -> {
			try {
				maintainReplicas();
			} catch (Exception e) {
//						e.getMessage();
//						e.printStackTrace();
			}
		}, 0, 30, TimeUnit.SECONDS);
	}

	private static void parseCommand(String[] args) {
		if (args != null && args.length == 3) {
			try { 
				workerPortNo = Integer.parseInt(args[0]);
				storagePath = args[1];
				coordinatorEntry = args[2];
			} catch (NumberFormatException e) { // TODO error handling for command-line argument
				System.out.println("Invalid Input. Expected a port number for the worker,"
						+ " a storage directory, the IP and port of the coordinator!");
				System.exit(1); // exit the program
			}
		} else {
			System.out.println("Invalid Input. Expected a port number for the worker,"
					+ " a storage directory, the IP and port of the coordinator!");
			System.exit(1); // exit the program
		}
	}
	
	/**
	 * Parses the command-line argument and passes it
	 * to the web server's port function.
	 */
	private static void init(String[] args) {
		// parse the command line 
		parseCommand(args);
		// get id from the id file
		workerId = getWorkerId();
		// pass port argument to web server
		port(workerPortNo); 
		// instantiate a KVSClient (EC)
		kvsClient = new KVSClient(coordinatorEntry);
	}

	private static String getWorkerId() {
		try {
            File idFile = new File(storagePath, "id");
            if (idFile.exists()) {
                BufferedReader reader = new BufferedReader(new FileReader(idFile));
                String workerId = reader.readLine();
                reader.close();
                return workerId;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
	}
	
	private static void handlePutRow() {
	    put("/data/:T", (req, res) -> {
	        try {
	            // get the tableName from the request parameters
	            String tableName = req.params("T");

	            // get the row data from the request body
	            InputStream bodyInputStream = new ByteArrayInputStream(req.bodyAsBytes());

	            // parse the row data into a Row object
	            Row row = Row.readFrom(bodyInputStream);

	            // check if row is null (indicating a parsing error or end of stream)
	            if (row == null) {
	                res.status(400, "Bad Request"); // Bad Request
	                return "Bad Request: Invalid row data";
	            }

	            // put the row into the database
	            putRow(tableName, row, storagePath);

	            // set the response status to 200 OK and return a success message
	            res.status(200, "OK");
//	            return "Row inserted successfully into table " + tableName;
	            return "OK";
	        } catch (Exception e) {
	            res.status(500, "Internal Server Error"); // Internal Server Error
	            return "Error processing request: " + e.getMessage();
	        }
	    });
	}
	
	/**
	 * Gets parameters from request and put data into database (in-memory).
	 */
	private static void getParamsAndPutData() {
		put("/data/:T/:R/:C", (req, res) -> {
			// extract the parameters from request
			Map<String, String> params = req.params();
			String table = params.get("T");
			String row = params.get("R");
			String col = params.get("C");
			// add two additional query parameters (EC) for CPUT
			String ifColumn = req.queryParams("ifcolumn");
			String equals = req.queryParams("equals");
			String version = req.queryParams("version");
			Integer versionNo = null;
			if (version != null) {
				try {
					versionNo = Integer.parseInt(version);
				} catch (NumberFormatException e) { // versionNo is not valid
					res.status(400, "Bad Request");
					return null;
				}
			} else {
				// get latest version number
				versionNo = getLatestVersionNo(table, row);
			}
			if (ifColumn != null && equals != null) { // check whether the data exists
				// get data from the database
				byte[] data = getData(table, row, ifColumn, versionNo);
				if (data == null || !equals.equals(new String(data, "UTF-8"))) {
					// set status and response
					res.status(200, "OK");
					return "FAIL";
				}
			}
			byte[] bodyAsBytes = req.bodyAsBytes();
			// put data in the database
			try {
				putData(table, row, col, bodyAsBytes);
			} catch (IOException e) {
				res.status(500, "Internal Server Error");
				return null;
			}
			versionNo = getLatestVersionNo(table, row); // get latest version of the Row after update
			if (versionNo != null) { // set Version header
				res.header("Version", versionNo.toString());
			}
			// EC (replication)
			String replication = req.queryParams("replication");
			if (replication == null) {
				handleReplications(table, row, col, bodyAsBytes, null, "PUT");
			}
			
			// set status and response
			res.status(200, "OK");
			return "OK";
		});
	}
	
	private static void getParamasAndGetData() {
		get("/data/:T/:R/:C", (req, res) -> { 
			// extract the parameters from request
			Map<String, String> params = req.params();
			String table = params.get("T");
			String row = params.get("R");
			String col = params.get("C");
			logger.debug("GET request for table " + table + ", row " + row + ", column " + col);
			String version = req.queryParams("version");
			Integer versionNo = null;
			if (version != null) {
				try {
					versionNo = Integer.parseInt(version);
				} catch (NumberFormatException e) { // versionNo is not valid
					res.status(400, "Bad Request");
					res.body("404 Not Found");
					return null;
				}
			} else {
				// get latest version number
				versionNo = getLatestVersionNo(table, row);
			}
			// get data from the database
			byte[] data = getData(table, row, col, versionNo);
			// error handling
			if (data != null) { // successfully get data from database
				res.bodyAsBytes(data); // set response body as bytes
				if (versionNo != null) { // set Version header
					res.header("Version", versionNo.toString());
				}
			} else { // 404 NOT FOUND
				res.status(404, "Not Found"); // set 404 error
				res.body("404 Not Found");
			}
			return null;
		});
	}
	
	
	/**
	 * Gets the latest version number from the database with given table and row.
	 * @param table
	 * @param row
	 * @return
	 */
	private static Integer getLatestVersionNo(String table, String row) {
		try {
			return database.get(table).get(row).size();
		} catch (NullPointerException e) {
			return 0;
		}
		
	}

	/**
	 * Puts data into the database with given tableName, rowName and colName.
	 * @param tableName
	 * @param rowName
	 * @param colName
	 * @param data
	 */
	private static void putData(String tableName, String rowName, String colName, byte[] data) throws IOException {
		// create a new Row Object
		Row newRow = new Row(rowName);
		// get columns
		Map<String, byte[]> rowEntry = newRow.values;
		Integer latestVersionNo = getLatestVersionNo(tableName, rowName);
//		System.out.println(latestVersionNo.toString());
		Row lastVersionRow = getRow(tableName, rowName, latestVersionNo, storagePath);
		if (lastVersionRow != null) {
			for (Entry<String, byte[]> entry : lastVersionRow.values.entrySet()) { // copy data from last version
				rowEntry.put(entry.getKey(), entry.getValue());
			}
		}
		// put data in
		rowEntry.put(colName, data);
		// put the row in the database
		putRow(tableName, newRow, storagePath);
	}
	
	/**
	 * Gets data from the database with given tableName, rowName, colName.
	 * @param tableName
	 * @param rowName
	 * @param colName
	 * @return data as bytes.
	 */
	private static byte[] getData(String tableName, String rowName, String colName, Integer versionNo) {
		Row row = getRow(tableName, rowName, versionNo, storagePath); // get Row from database using T and K
		if (row != null && row.values != null && row.values.containsKey(colName)) { // database has data entry
			return row.values.get(colName);
		}
		
		return null;
	}
	
	/**
	 * Gets the tables meta data.
	 */
	private static void getTablesMetaData() {
		get("/", (req, res)->{
			String htmlPage = generateHTMLPage("/", null, storagePath, workerPortNo, null);
			if (htmlPage == null) { // cannot generate html page server internal error
				res.status(500, "Internal Server Error");
				res.body("500 Internal Server Error");
				return null;
			}
			res.type("text/html");
			res.status(200, "OK");
			return htmlPage;
		});
	}
	
	/**
	 * Gets table's view.
	 */
	private static void getTableView() {
		// deal with bad request (null) and Not Found TODO
		get("/view/:T", (req, res) -> {
			String tableName = req.params("T");
			if (tableName == null) {
				res.status(400, "Bad Request");
				res.body("400 Bad Request");
				return null;
			}
			String fromRow = req.queryParams("fromRow");
			String htmlPage = generateHTMLPage("/view", tableName, storagePath, workerPortNo, fromRow);
			if (htmlPage == null) {
				res.status(500, "Server Internal Error");
				res.body("500 Internal Server Error");
				return null;
			}
			res.type("text/html");
			res.status(200, "OK");
			
			return htmlPage;
		});
	}
	
	/**
	 * Reads the whole row.
	 */
	private static void getWholeRow() {
		get("/data/:T/:R", (req, res) ->{
			// get parameters from req
			Map<String, String> paramsMap = req.params();
			if (paramsMap == null) {
				res.status(400, "Bad Request");
				res.body("400 Bad Request");
				return null;
			}
			String tableName = paramsMap.get("T");
			String key = paramsMap.get("R");
			logger.debug("GET request for table " + tableName + ", row " + key);
			// TODO version number for persistent data
			String version = req.queryParams("version");
			Integer versionNo = null;
			if (version != null) {
				try {
					versionNo = Integer.parseInt(version);
				} catch (NumberFormatException e) { // versionNo is not valid
					res.type("text/plain");
					res.status(400, "Bad Request");
					res.body("400 Bad Request");
					return null;
				}
			} else {
				// get latest version number
				versionNo = getLatestVersionNo(tableName, key);
			}
			// get row from database and disk as bytes
			Row row = getRow(tableName, key, null, storagePath);
			logger.debug("Row: " + row);
			// error handling
			if (row != null) { // successfully get data from database
				res.bodyAsBytes(row.toByteArray()); // set response body as bytes
				res.type("application/octet-stream");
				if (versionNo != null) { // set Version header
					res.header("Version", versionNo.toString());
				}
				res.status(200, "OK");
			} else { // 404 NOT FOUND
				res.type("text/plain");
				res.status(404, "Not Found"); // set 404 error
				res.body("404 Not Found");
			}

			return null;
		});
	}
	
	/**
	 * Performs streaming read.
	 */
	private static void readStream() {
		get("/data/:T", (req, res) -> {
			String tableName = req.params("T");
			if (tableName == null) {
				res.status(400, "Bad Request");
				res.body("400 Bad Request");
				return null;
			}
			if (hasTable(tableName) == null) { // table does not exist
				res.status(404, "Not Found");
				res.body("404 Not Found");
				return null;
			}
			String startRow = req.queryParams("startRow");
			String endRowExclusive = req.queryParams("endRowExclusive");
			String hash = req.queryParams("hash"); // for EC
			if (hash == null) {
				getAndSendStream(res, tableName, startRow, endRowExclusive);
			} else { // EC
				TreeSet<Row> rowsSet = new TreeSet<>((r1, r2) -> { // override comparator for TreeSet
					return r1.key().compareTo(r2.key());
				});
		        getAllColumnsAndRowsSet(new TreeSet<String>(), rowsSet, tableName, storagePath, null);
				StringBuilder sb = new StringBuilder();
				sb.append(rowsSet.size() + "\n");
//					try {
//						System.out.println(String.valueOf(rowsSet.size()).getBytes());
//						res.write(String.valueOf(rowsSet.size()).getBytes());
//						res.write(new byte[] {(byte) 10}); // LF character
//					} catch (Exception e) {
//						e.printStackTrace();
//					}
				for (Row row : rowsSet) {
		            if ((startRow == null || row.key.compareTo(startRow) >= 0) &&
		                (endRowExclusive == null || row.key.compareTo(endRowExclusive) < 0)) {
		            	String rowAndHash = row.key() + "," + HashUtil.generateSHA256(row.toByteArray());
//			                try {
//								res.write(rowAndHash.getBytes());
//								res.write(new byte[] {(byte) 10}); // LF character
//							} catch (Exception e) {
//								e.printStackTrace();
//							}
		            	sb.append(rowAndHash + "\n");
		            }
		        }
				res.status(200, "OK");
				res.type("text/plain");
				return sb.toString();
			}
			
			return null;
		});
	}
	
	/**
	 * Get route for "/count/XXX".
	 */
	private static void getRowCount() {
		get("/count/:T", (req, res) -> {
			String tableName = req.params("T");
			if (tableName == null) {
				res.status(400, "Bad Request");
				res.body("400 Bad Request");
				return null;
			}
			Long count = hasTable(tableName);
			if (count != null) { // table exists
				res.status(200, "OK");
				res.body(String.valueOf(count));
			} else { // table does not exist
				res.status(404, "Not Found");
				res.body("404 Not Found");
			}
			return null;
		});
	}
	
	/**
	 * Return the names of all the tables the worker knows about, including any persistent tables.
	 */
	private static void getTableList() {
		get("/tables", (req, res) -> {
			TreeMap<String, Long> tablesMetaData = getTableNames(storagePath);
			res.type("text/plain");
			StringBuilder sb = new StringBuilder();
			for (String table : tablesMetaData.keySet()) {
				sb.append(table);
				sb.append('\n');
			}
	        res.status(200, "OK");
			
			return sb.toString();
		});
	}
	
	/**
	 * Renames a table.
	 */
	private static void renameTable() {
		put("/rename/:T", (req, res) -> {
			String oldName = req.params("T");
			String newName = req.body();
			// error handling
			if (oldName == null || newName == null) {
				res.status(400, "Bad Request");
				res.body("400 Bad Request");
				return null;
			} else if (oldName.startsWith("pt-") && !newName.startsWith("pt-") 
					|| !oldName.startsWith("pt-") && newName.startsWith("pt-")) {
				res.status(400, "Bad Request");
				res.body("400 Bad Request");
				return null;
			}
			TreeMap<String, Long> tablesMetaData = getTableNames(storagePath);
			if (!tablesMetaData.containsKey(oldName)) { // 404 Not Found
				res.status(404, "Not Found");
				res.body("404 Not Found");
				return null;
			} else if (tablesMetaData.containsKey(newName)) {
				res.status(409, "Conflict");
				res.body("409 Conflict");
				return null;
			} 
			try {
				if (oldName.startsWith("pt-")) {
					rename(oldName, newName, true);
				} else {
					rename(oldName, newName, false);
				}
			} catch (Exception e) {
				e.printStackTrace();
				res.status(500, "Internal Server Error");
				res.body("500 Internal Server Error");
				return null;
			}
			
			
			// EC (replication)
			String replication = req.queryParams("replication");
			if (replication == null) {
				handleReplications(oldName, null, null, null, newName, "RENAME");
			}
			
			res.status(200, "OK");
			return "OK";
		});
	}
	
	/**
	 * Deletes a table from the database (in memory or in disk).
	 */
	private static void deleteTable() {
		put("/delete/:T", (req, res) -> {
			String tableName = req.params("T");
			if (tableName == null) {
				res.status(400, "Bad Request");
				res.body("400 Bad Request");
				return null;
			}
			TreeMap<String, Long> tablesMetaData = getTableNames(storagePath);
			if (!tablesMetaData.containsKey(tableName)) { // 404 Not Found
				res.status(404, "Not Found");
				res.body("404 Not Found");
				return null;
			}
			try {
				delete(tableName);
			} catch (Exception e) {
				e.printStackTrace();
				res.status(500, "Internal Server Error");
				res.body("500 Internal Server Error");
				return null;
			}
			// EC (replication)
			String replication = req.queryParams("replication");
			if (replication == null) {
				handleReplications(tableName, null, null, null, null, "DELETE");
			}
			
			res.status(200, "OK");
			return "OK";
		});
	}
	
	/**
	 * Deletes the table.
	 * @param tableName
	 */
	private static void delete(String tableName) throws Exception {
		// for persistent table
		if (tableName.startsWith("pt-")) {
			Path tablePath = Paths.get(storagePath, tableName);
	        // delete all files within the directory
			Files.walkFileTree(tablePath, new SimpleFileVisitor<Path>() {
				@Override
				public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
					Files.delete(file);
					return FileVisitResult.CONTINUE;
				}

				@Override
				public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
					Files.delete(dir);
					return FileVisitResult.CONTINUE;
				}
			});
		} else {
			database.remove(tableName);
		}
		
	}

	/**
	 * Renames the table from oldName to newName.
	 * @param oldName
	 * @param newName
	 * @param isPt
	 * @throws Exception
	 */
	private static void rename(String oldName, String newName, boolean isPt) throws Exception {
		// for persistent table
		if (isPt) {
			Path oldPath = Paths.get(storagePath, oldName);
		    Path newPath = Paths.get(storagePath, newName);
		    // rename table
		    Files.move(oldPath, newPath);
		} else { // for in memory database
			database.put(newName, database.get(oldName));
			database.remove(oldName);
		}
	}

	/**
	 * Checks if the given tableName exists in memory or in disk.
	 * @param tableName
	 * @return the count of rows in the table.
	 */
	private static Long hasTable(String tableName) {
		TreeMap<String, Long> tablesMetaData = getTableNames(storagePath);
		if (tablesMetaData != null) {
			return tablesMetaData.get(tableName);
		}
		return null;
	}

	/**
	 * Gets qualified stream and send it as a response.
	 * @param res
	 * @param tableName
	 * @param startRow
	 * @param endRowExclusive
	 */
	private static void getAndSendStream(Response res, String tableName, String startRow, String endRowExclusive) {
		TreeSet<Row> rowsSet = new TreeSet<>((r1, r2) -> { // override comparator for TreeSet
			return r1.key().compareTo(r2.key());
		});
        getAllColumnsAndRowsSet(new TreeSet<String>(), rowsSet, tableName, storagePath, null);
        for (Row row : rowsSet) {
            if ((startRow == null || row.key.compareTo(startRow) >= 0) &&
                (endRowExclusive == null || row.key.compareTo(endRowExclusive) < 0)) {
                byte[] rowData = row.toByteArray();  // Replace with actual deserialization method
                try {
					res.write(rowData);
					res.write(new byte[] {(byte) 10}); // LF character
				} catch (Exception e) {
					e.printStackTrace();
				}
            }
        }
        // End of the stream
        try {
			res.write(new byte[] {(byte) 10}); // LF character
		} catch (Exception e) {
			e.printStackTrace();
		}
        res.status(200, "OK");
        res.type("application/octet-stream");
//        res.type("text/plain");
	}
	
	/**
	 * Handles replications when put request come (EC).
	 * Finds the two workers with next lower IDs.
	 * Forwards the PUT request to them.
	 */
	private static void handleReplications(String tableName, String row, String col, byte[] value, String newName, String operation) {
		// find the two workers with next lower IDs
		int[] nextTwoLowerWorkers = getNextTwoLowerWorkers();
		// forward the put request to the workers
		if (nextTwoLowerWorkers != null) {
			for (int workerIdx : nextTwoLowerWorkers) {
				if (workerIdx != -1) { // valid worker id
					if (operation.equals("PUT")) {
						try {
							String target = "http://"+kvsClient.workers.elementAt(workerIdx).address+"/data/"+tableName+"/"+java.net.URLEncoder.encode(row, "UTF-8")+"/"+java.net.URLEncoder.encode(col, "UTF-8") + "/?replication=true";
						    byte[] response = HTTP.doRequest("PUT", target, value).body();
						    String result = new String(response);
						    if (!result.equals("OK")) 
						    	System.out.println("PUT returned something other than OK: " + result + "("+target+")");
						} catch (Exception e) {
						} 
					} else if (operation.equals("DELETE")) {
						try {
					        byte[] response = HTTP.doRequest("PUT", "http://"+kvsClient.workers.elementAt(workerIdx).address+"/delete/"+java.net.URLEncoder.encode(tableName, "UTF-8")+"/?replication=true", null).body();
					        String result = new String(response);
					        if (!result.equals("OK")) 
						    	System.out.println("PUT returned something other than OK: " + result);
					      } catch (Exception e) {
					      }
					} else if (operation.equals("RENAME")) {
						try {
					        byte[] response = HTTP.doRequest("PUT", "http://"+kvsClient.workers.elementAt(workerIdx).address+"/rename/"+java.net.URLEncoder.encode(tableName, "UTF-8")+"/?replication=true", newName.getBytes()).body();
					        String res = new String(response);
					        if (!res.equals("OK")) 
						    	System.out.println("PUT returned something other than OK: " + res);
					      } catch (Exception e) {}
					}
				}
			}
		}
	}

	/**
	 * Gets next two lower workers.
	 * @return
	 */
	private static int[] getNextTwoLowerWorkers() {
		int[] nextTwoLowerWorkersIdx = new int[] {-1, -1};
//		System.out.println(kvsClient.workerIndexForKey(workerId));
//		System.out.println(workerId);
//		try {
//			System.out.println(kvsClient.numWorkers());
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
		try {
			if (kvsClient.numWorkers() == 0) {
				return null;
			}
		} catch (Exception e) {
			return null;
		}
		int idx = Collections.binarySearch(kvsClient.workers, kvsClient.workers.elementAt(kvsClient.workerIndexForKey(workerId)));
		
		if (idx < 0) {
			System.out.println("cannot find worker!");
			idx = -idx - 1; // the position the worker will be added
		}
		
		try {
			if (kvsClient.numWorkers() == 2) {
				if (idx == 0) {
					nextTwoLowerWorkersIdx[0] = 1;
				} else if (idx == 1) {
					nextTwoLowerWorkersIdx[0] = 0;
				}
			} else if (kvsClient.numWorkers() >= 3) {
				// handle edge cases (wrap around if necessary)
		        if (idx == 0) {
		        	nextTwoLowerWorkersIdx[0] = kvsClient.numWorkers() - 1;
		        	nextTwoLowerWorkersIdx[1] = kvsClient.numWorkers() - 2;
		        } else if (idx == 1) {
		        	nextTwoLowerWorkersIdx[0] = 0;
		        	nextTwoLowerWorkersIdx[1] = kvsClient.numWorkers() - 1;
		        } else {
		        	nextTwoLowerWorkersIdx[0] = idx - 1;
		        	nextTwoLowerWorkersIdx[1] = idx - 2;
		        }
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return nextTwoLowerWorkersIdx;
	}
	
	/**
	 * Maintenance for replicas.
	 * @throws IOException
	 */
	private synchronized static void maintainReplicas() {
		
		int[] nextTwoHigherWorkers = getNextTwoHigherWorkers();
		
		if (nextTwoHigherWorkers != null) {
			for (int idx : nextTwoHigherWorkers) {
				if (idx == -1) continue;
				String[] tables = null;
				try {
					String result = new String(HTTP.doRequest("GET", "http://"+kvsClient.workers.elementAt(idx).address+"/tables", null).body());
//					System.out.println(result);
				    tables = result.split("\n");
//				    System.out.println(tables.length);
				} catch (Exception e) {}
			    // for each table
			    for (String table : tables) {
			    	int numRows = 0;
			    	String[] pieces = null;
			    	try {
			    		String result = new String(HTTP.doRequest("GET", "http://"+kvsClient.workers.elementAt(idx).address+"/data/" + table + "?hash=true", null).body());
				    	pieces = result.split("\n");
				    	numRows = Integer.parseInt(pieces[0]);
			    	} catch (Exception e) {}
			    	if (numRows < 1) break;
			        if (pieces == null || pieces.length != (numRows + 1)) break;
			        for (int i = 0; i < numRows; i++) {
			        	// parse the response
				        String[] pcs = pieces[1 + i].split(",");
				        String key = pcs[0]; // get row key
				        String hash = pcs[1]; // get row hash
//				        System.out.println(key);
				        // getRow
				        Row row = getRow(table, key, null, storagePath);
				        String startKey = kvsClient.workers.elementAt(idx).id; 
				        String endKey = null;
				        // check if the row.key is within the range of the worker
				        try {
				        	if (idx == kvsClient.numWorkers() - 1) {
					        	endKey = kvsClient.workers.elementAt(0).id;
					        } else {
					        	endKey = kvsClient.workers.elementAt(idx + 1).id;
					        }
				        } catch (Exception e) {}
				        if (startKey != null && endKey != null && startKey.compareTo(endKey) < 0) {
				        	if (key.compareTo(startKey) < 0) continue;
					        if (key.compareTo(endKey) >= 0) continue;
				        } else if (startKey != null && endKey != null && startKey.compareTo(endKey) > 0) {
				        	if (key.compareTo(startKey) >= 0 && key.compareTo(endKey) < 0) continue;
				        } 
				        // maintenance check
				        if (row == null || HashUtil.generateSHA256(row.toByteArray()) != hash) {
				        	// update storage
				        	try {
//				        		System.out.println("123");
//				        		byte[] body = HTTP.doRequest("GET", "http://" + kvsClient.workers.elementAt(idx).address+"/data/" + table + "/" + key, null).body();
//					        	InputStream in = new ByteArrayInputStream(body);
//					        	Row newRow = Row.readFrom(in);
				        		Row newRow = kvsClient.getRow(table, key);
					        	putRow(table, newRow, storagePath);
				        	} catch (Exception e) {}
				        }
			        }
			    }
			}
		}
	}

	private static int[] getNextTwoHigherWorkers() {
		int[] nextTwoHigherWorkers = new int[] {-1, -1};
		try {
			if (kvsClient.numWorkers() == 0) {
				return null;
			}
		} catch (Exception e) {
			return null;
		}
		int idx = Collections.binarySearch(kvsClient.workers, kvsClient.workers.elementAt(kvsClient.workerIndexForKey(workerId)));
		
		if (idx < 0) {
			System.out.println("cannot find worker!");
			idx = -idx - 1; // the position the worker will be added
		}
		
		try {
			if (kvsClient.numWorkers() == 2) {
				if (idx == 0) {
					nextTwoHigherWorkers[0] = 1;
				} else if (idx == 1) {
					nextTwoHigherWorkers[0] = 0;
				}
			} else if (kvsClient.numWorkers() >= 3) {
				// handle edge cases (wrap around if necessary)
		        if (idx == kvsClient.numWorkers() - 1) {
		        	nextTwoHigherWorkers[0] = 0;
		        	nextTwoHigherWorkers[1] = 1;
		        } else if (idx == kvsClient.numWorkers() - 2) {
		        	nextTwoHigherWorkers[0] = kvsClient.numWorkers() - 1;
		        	nextTwoHigherWorkers[1] = 0;
		        } else {
		        	nextTwoHigherWorkers[0] = idx + 1;
		        	nextTwoHigherWorkers[1] = idx + 2;
		        }
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return nextTwoHigherWorkers;
	}
	
}
