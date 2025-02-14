package cis5550.flame;

import java.util.*;
import java.util.Map.Entry;
import java.net.*;
import java.io.*;

import static cis5550.webserver.Server.*;
import cis5550.tools.*;
import cis5550.flame.FlameContext.RowToString;
import cis5550.flame.FlamePairRDD.*;
import cis5550.flame.FlameRDD.*;
import cis5550.kvs.*;
import cis5550.webserver.Request;

class Worker extends cis5550.generic.Worker { // TODO change List<> to Iterator to save memory space
	
	public static class TransformingIterator implements Iterator<String> {
	    private Iterator<Row> original;

	    public TransformingIterator(Iterator<Row> original) {
	        this.original = original;
	    }

	    @Override
	    public boolean hasNext() {
	        return original.hasNext();
	    }

	    @Override
	    public String next() {
	        if (!hasNext()) {
	            throw new NoSuchElementException();
	        }
	        return original.next().get("value");
	    }
	}


	public static void main(String args[]) {
    if (args.length != 2) {
    	System.err.println("Syntax: Worker <port> <coordinatorIP:port>");
    	System.exit(1);
    }

    int port = Integer.parseInt(args[0]);
    String server = args[1];
	startPingThread(server, port, ""+port, null);
    final File myJAR = new File("__worker"+port+"-current.jar");

  	port(port);

    post("/useJAR", (request,response) -> {
      FileOutputStream fos = new FileOutputStream(myJAR);
      fos.write(request.bodyAsBytes());
      fos.close();
      return "OK";
    });
    
    // define post route for flatMap
    post("/rdd/:O", (request, response) -> {
    	String operation = request.params("O");
    	if (operation == null) {
    		response.status(400, "Bad Request");
    		response.body("Bad Operation Request");
    		return null;
    	}
    	String inputTable = request.queryParams("inputTable");
    	String outputTable = request.queryParams("outputTable");
    	String kvsCoordinator = request.queryParams("kvsCoordinator");
//    	String jarName = request.queryParams("jarName");
    	String fromKey = request.queryParams("fromKey");
    	String toKeyExclusive = request.queryParams("toKeyExclusive");
    	byte[] serializedLambda = request.bodyAsBytes();
    	// error handling
    	if (inputTable == null) {
    		response.status(400, "Bad Request");
    		response.body("Bad Request: Miss inputTable");
    		return null;
    	}
    	if (outputTable == null) {
    		response.status(400, "Bad Request");
    		response.body("Bad Request: Miss outputTable");
    		return null;
    	}
    	if (kvsCoordinator == null) {
    		response.status(400, "Bad Request");
    		response.body("Bad Request: Miss kvsCoordinator");
    		return null;
    	}
//    	File jarFile = null;
//    	if (jarName != null) {
//    		jarFile = new File(jarName);
//    	}
    	KVSClient kvsClient = new KVSClient(kvsCoordinator);
    	
    	if (operation.equals("flatMap")) {
    		String pairRDD = request.queryParams("pairRDD");
    		if (pairRDD != null && pairRDD.equalsIgnoreCase("true")) { // pairRDD to String Iterable
    			PairToStringIterable lambda = (PairToStringIterable) Serializer.byteArrayToObject(serializedLambda, myJAR);
    			
    			if (lambda == null) {
            		response.status(400, "Bad Request");
            		response.body("Bad Request: Miss the serialized lambda in the requestBody");
            		return null;
            	}
//            	KVSClient kvsClient = new KVSClient(kvsCoordinator);
            	Iterator<Row> rows = kvsClient.scan(inputTable, fromKey, toKeyExclusive);
            	
            	while (rows.hasNext()) {
            		Row row = rows.next();
            		
            		for (String column : row.columns()) {
            			Iterable<String> results = lambda.op(new FlamePair(row.key(), row.get(column)));
                		if (results != null) {
                			for (String result : results) {
                				String uniqueRowKey = generateUniqueRowKey(row.key());
                				kvsClient.put(outputTable, uniqueRowKey, "value", result);
                			}
                		}
            		}
            	}	
    		} else { // RDD to String Iterable
    			// de-serialize lambda
//            	StringToIterable lambda = (StringToIterable) Serializer.byteArrayToObject(serializedLambda, jarFile);
            	StringToIterable lambda = (StringToIterable) Serializer.byteArrayToObject(serializedLambda, myJAR);
            	if (lambda == null) {
            		response.status(400, "Bad Request");
            		response.body("Bad Request: Miss the serialized lambda in the requestBody");
            		return null;
            	}
//            	KVSClient kvsClient = new KVSClient(kvsCoordinator);
            	Iterator<Row> rows = kvsClient.scan(inputTable, fromKey, toKeyExclusive);
            	
            	while (rows.hasNext()) {
            		Row row = rows.next();
            		Iterable<String> results = lambda.op(row.get("value"));
            		if (results != null) {
            			for (String result : results) {
            				String uniqueRowKey = generateUniqueRowKey(row.key());
            				kvsClient.put(outputTable, uniqueRowKey, "value", result);
            			}
            		}
            	}
    		}
    	} else if (operation.equals("mapToPair")) {
    		// de-serialize lambda
//    		StringToPair lambda = (StringToPair) Serializer.byteArrayToObject(serializedLambda, jarFile);
    		StringToPair lambda = (StringToPair) Serializer.byteArrayToObject(serializedLambda, myJAR);
    		if (lambda == null) {
        		response.status(400, "Bad Request");
        		response.body("Bad Request: Miss the serialized lambda in the requestBody");
        		return null;
        	}
//    		KVSClient kvsClient = new KVSClient(kvsCoordinator);
        	Iterator<Row> rows = kvsClient.scan(inputTable, fromKey, toKeyExclusive);
        	
        	while (rows.hasNext()) {
        		Row row = rows.next();
        		for (String column : row.columns()) {
        			FlamePair flamePair = lambda.op(row.get(column));
            		if (flamePair != null) {
        				kvsClient.put(outputTable, flamePair._1(), row.key(), flamePair._2());
            		}
        		}
        	}
    	} else if (operation.equals("foldByKey")) {
    		String zeroElement = request.queryParams("zeroElement");
    		if (zeroElement == null) {
    			response.status(400, "Bad Request");
        		response.body("Bad Request: Miss the accumulator (zeroElement)");
        		return null;
    		}
    		// de-serialize lambda
//    		TwoStringsToString lambda = (TwoStringsToString) Serializer.byteArrayToObject(serializedLambda, jarFile);
    		TwoStringsToString lambda = (TwoStringsToString) Serializer.byteArrayToObject(serializedLambda, myJAR);
    		if (lambda == null) {
        		response.status(400, "Bad Request");
        		response.body("Bad Request: Miss the serialized lambda in the requestBody");
        		return null;
        	}
//    		KVSClient kvsClient = new KVSClient(kvsCoordinator);
        	Iterator<Row> rows = kvsClient.scan(inputTable, fromKey, toKeyExclusive);
        	
        	while (rows.hasNext()) {
        		Row row = rows.next();
        		String result = zeroElement;
        		for (String column : row.columns()) {
        			result = lambda.op(result, row.get(column));
        		}
        		if (result != null) {
    				kvsClient.put(outputTable, row.key(), row.key(), result);
        		}
        	}
    	} else if (operation.equals("intersection")) {
    		String anotherTable = request.queryParams("anotherTable");
    		if (anotherTable == null) {
    			response.status(400, "Bad Request");
        		response.body("Bad Request: Miss anotherRDD (anotherTable)");
        		return null;
    		}
//    		KVSClient kvsClient = new KVSClient(kvsCoordinator);
    		Iterator<Row> firstRows = kvsClient.scan(inputTable, fromKey, toKeyExclusive);
    		Iterator<Row> secondRows = kvsClient.scan(anotherTable, fromKey, toKeyExclusive);
    		List<Row> commonRows = findIntersection(firstRows, secondRows);
    		
    		for (Row row : commonRows) {
    			kvsClient.put(outputTable, row.key(), "value", row.get("value"));
    		}
    	} else if (operation.equals("sample")) {
    		String p = request.queryParams("propability");
    		if (p == null) {
    			response.status(400, "Bad Request");
        		response.body("Bad Request: Miss probability (f)");
        		return null;
    		}
//    		KVSClient kvsClient = new KVSClient(kvsCoordinator);
    		Double f = Double.parseDouble(request.queryParams("propability"));
    		Iterator<Row> originalData = kvsClient.scan(inputTable, fromKey, toKeyExclusive);
    		List<Row> sampledData;
    		try {
    			sampledData = generateSampleData(originalData, f);
    		} catch (IllegalArgumentException e) {
    			response.status(400, "Bad Request");
        		response.body("Bad Request: Illgal probability range (0.0 - 1.0)");
        		return null;
    		}
    		if (sampledData != null) {
    			for (Row row : sampledData) {
    				kvsClient.put(outputTable, row.key(), "value", row.get("value"));
    			}
    		}
    	} else if (operation.equals("groupBy")) {
    		// de-serialize lambda
//    		StringToString lambda = (StringToString) Serializer.byteArrayToObject(serializedLambda, jarFile);
    		StringToString lambda = (StringToString) Serializer.byteArrayToObject(serializedLambda, myJAR);
    		if (lambda == null) {
        		response.status(400, "Bad Request");
        		response.body("Bad Request: Miss the serialized lambda in the requestBody");
        		return null;
        	}
//    		KVSClient kvsClient = new KVSClient(kvsCoordinator);
        	Iterator<Row> rows = kvsClient.scan(inputTable, fromKey, toKeyExclusive);
        	Map<String, List<String>> groupMap = getGroupMap(rows, lambda);
        	
        	for (Entry<String, List<String>> entry : groupMap.entrySet()) {
        		String key = entry.getKey();
        		String value = String.join(",", entry.getValue());
        		kvsClient.put(outputTable, key, "value", value);
        	}
    	} else if (operation.equals("fromTable")) {
    		// de-serialize lambda
    		RowToString lambda = (RowToString) Serializer.byteArrayToObject(serializedLambda, myJAR);
    		if (lambda == null) {
        		response.status(400, "Bad Request");
        		response.body("Bad Request: Miss the serialized lambda in the requestBody");
        		return null;
        	}
//    		KVSClient kvsClient = new KVSClient(kvsCoordinator);
    		// scan the range of the input table that the worker has been assigned
    		Iterator<Row> rows = kvsClient.scan(inputTable, fromKey, toKeyExclusive);
    		
    		while (rows.hasNext()) {
    			Row row = rows.next();
    			
    			String result = lambda.op(row);
    			if (result != null) {
    				kvsClient.put(outputTable, row.key(), "value", result);
    			}
    		}
    	} else if (operation.equals("flatMapToPair")) {
    		String pairRDD = request.queryParams("pairRDD");
    		if (pairRDD != null && pairRDD.equalsIgnoreCase("true")) { // PairToPairIterable
    			// de-serialize lambda
    			PairToPairIterable lambda = (PairToPairIterable) Serializer.byteArrayToObject(serializedLambda, myJAR);
        		if (lambda == null) {
            		response.status(400, "Bad Request");
            		response.body("Bad Request: Miss the serialized lambda in the requestBody");
            		return null;
            	}
//        		KVSClient kvsClient = new KVSClient(kvsCoordinator);
        		// scan the range of the input table that the worker has been assigned
        		Iterator<Row> rows = kvsClient.scan(inputTable, fromKey, toKeyExclusive);
        		
        		while (rows.hasNext()) {
        			Row row = rows.next();
        			
        			for (String column : row.columns()) {
        				Iterable<FlamePair> results = lambda.op(new FlamePair(row.key(), row.get(column)));
                		if (results != null) {
                			for (FlamePair result : results) {
                				String uniqueCol = generateUniqueRowKey(row.key());
                				kvsClient.put(outputTable, result._1(), uniqueCol, result._2());
                			}
                		}
        			}
        		}
    		} else { // StringToPairIterable
    			// de-serialize lambda
        		StringToPairIterable lambda = (StringToPairIterable) Serializer.byteArrayToObject(serializedLambda, myJAR);
        		if (lambda == null) {
            		response.status(400, "Bad Request");
            		response.body("Bad Request: Miss the serialized lambda in the requestBody");
            		return null;
            	}
//        		KVSClient kvsClient = new KVSClient(kvsCoordinator);
        		// scan the range of the input table that the worker has been assigned
        		Iterator<Row> rows = kvsClient.scan(inputTable, fromKey, toKeyExclusive);
        		
        		while (rows.hasNext()) {
        			Row row = rows.next();
            		Iterable<FlamePair> results = lambda.op(row.get("value"));
            		if (results != null) {
            			for (FlamePair result : results) {
            				String uniqueCol = generateUniqueRowKey(row.key());
            				kvsClient.put(outputTable, result._1(), uniqueCol, result._2());
            			}
            		}
        		}
    		}
    	} else if (operation.equals("distinct")) {
//    		KVSClient kvsClient = new KVSClient(kvsCoordinator);
    		// scan the range of the input table that the worker has been assigned
    		Iterator<Row> rows = kvsClient.scan(inputTable, fromKey, toKeyExclusive);
    		
    		while (rows.hasNext()) {
    			Row row = rows.next();
				kvsClient.put(outputTable, row.get("value"), "value", row.get("value"));
    		}
    	} else if (operation.equals("join")) {
    		String anotherTable = request.queryParams("anotherTable");
    		if (anotherTable == null) {
    			response.status(400, "Bad Request");
        		response.body("Bad Request: Miss anotherPairRDD (anotherTable)");
        		return null;
    		}
//    		KVSClient kvsClient = new KVSClient(kvsCoordinator);
    		Iterator<Row> firstRows = kvsClient.scan(inputTable, fromKey, toKeyExclusive);
    		Iterator<Row> secondRows = kvsClient.scan(anotherTable, fromKey, toKeyExclusive);
    		List<Row> newRows = joinTables(firstRows, secondRows);
    		
    		for (Row row : newRows) {
    			for (String column : row.columns()) {
    				kvsClient.put(outputTable, row.key(), column, row.get(column));
    			}
    		}
    	} else if (operation.equals("fold")) {
    		String zeroElement = request.queryParams("zeroElement");
    		if (zeroElement == null) {
    			response.status(400, "Bad Request");
        		response.body("Bad Request: Miss the accumulator (zeroElement)");
        		return null;
    		}
    		// de-serialize lambda
    		TwoStringsToString lambda = (TwoStringsToString) Serializer.byteArrayToObject(serializedLambda, myJAR);
    		if (lambda == null) {
        		response.status(400, "Bad Request");
        		response.body("Bad Request: Miss the serialized lambda in the requestBody");
        		return null;
        	}
//    		KVSClient kvsClient = new KVSClient(kvsCoordinator);
        	Iterator<Row> rows = kvsClient.scan(inputTable, fromKey, toKeyExclusive);
        	String result = zeroElement;
        	
        	while (rows.hasNext()) {
        		Row row = rows.next();

        		for (String column : row.columns()) {
        			result = lambda.op(result, row.get(column));
        		}
        	}
        	
        	response.status(200, "OK");
        	return result;	
    	} else if (operation.equals("filter")) { // EC filter
    		// de-serialize lambda
    		StringToBoolean lambda = (StringToBoolean) Serializer.byteArrayToObject(serializedLambda, myJAR);
    		if (lambda == null) {
        		response.status(400, "Bad Request");
        		response.body("Bad Request: Miss the serialized lambda in the requestBody");
        		return null;
        	}
//    		KVSClient kvsClient = new KVSClient(kvsCoordinator);
        	Iterator<Row> rows = kvsClient.scan(inputTable, fromKey, toKeyExclusive);
        	
        	while (rows.hasNext()) {
        		Row row = rows.next();

        		for (String column : row.columns()) {
        			Boolean predicate = lambda.op(row.get("value"));
        			if (predicate != null && predicate) {
        				kvsClient.put(outputTable, row.key(), "value", row.get("value"));
        			}
        		}
        	}
    	} else if (operation.equals("mapPartitions")) { // EC mapPartitions
    		// de-serialize lambda
    		IteratorToIterator lambda = (IteratorToIterator) Serializer.byteArrayToObject(serializedLambda, myJAR);
    		if (lambda == null) {
        		response.status(400, "Bad Request");
        		response.body("Bad Request: Miss the serialized lambda in the requestBody");
        		return null;
        	}
//    		KVSClient kvsClient = new KVSClient(kvsCoordinator);
        	Iterator<Row> rows = kvsClient.scan(inputTable, fromKey, toKeyExclusive);
        	
        	TransformingIterator elementsIterator = new TransformingIterator(rows);
        	
        	Iterator<String> results = lambda.op(elementsIterator);
        	
        	if (results != null) {
        		while (results.hasNext() && rows.hasNext()) {
            		Row row = rows.next();
            		String result = results.next();
            		kvsClient.put(outputTable, row.key(), "value", result);
            	}
        	}
    	} else if (operation.equals("cogroup")) { // EC cogroup
    		String anotherTable = request.queryParams("anotherTable");
    		if (anotherTable == null) {
    			response.status(400, "Bad Request");
        		response.body("Bad Request: Miss anotherPairRDD (anotherTable)");
        		return null;
    		}
//    		KVSClient kvsClient = new KVSClient(kvsCoordinator);
    		Iterator<Row> firstRows = kvsClient.scan(inputTable, fromKey, toKeyExclusive);
    		Iterator<Row> secondRows = kvsClient.scan(anotherTable, fromKey, toKeyExclusive);
    		List<Row> newRows = getCogroup(firstRows, secondRows); // TODO change to Iterator

    		for (Row row : newRows) {
    			kvsClient.put(outputTable, row.key(), "value", row.get("value"));
    		}
    	}
    	
    	response.status(200, "OK");
    	return "OK";
    });

	}

	private static List<Row> getCogroup(Iterator<Row> firstRows, Iterator<Row> secondRows) {
		Map<String, Row> rowsMap = new HashMap<>();
		Row curRow;
		
		while (firstRows.hasNext()) { // iterate first rows
			curRow = firstRows.next();
			String result = null;
			
			for (String column : curRow.columns()) {
				if (result == null)
					result = curRow.get(column);
				else
					result = combineValues(result, curRow.get(column));
			}
			if (result != null) {
				Row newRow = new Row(curRow.key());
				newRow.put("value", "[" + result + "],[]");
				rowsMap.put(curRow.key(), newRow);
			}
		}
		
		while (secondRows.hasNext()) { // iterate second rows (TODO)
			curRow = secondRows.next();
			String result = null;
			
			for (String column : curRow.columns()) {
				if (result == null)
					result = curRow.get(column);
				else
					result = combineValues(result, curRow.get(column));
			}
			
			if (result != null) {
				if (rowsMap.containsKey(curRow.key())) { // same key appeared in first rows
					Row row = rowsMap.get(curRow.key());
					String val = row.get("value");
					val = combineValues(val.substring(0, val.length() - 3), "[" + result + "]");
					row.put("value", val);
				} else { 
					Row newRow = new Row(curRow.key());
					newRow.put("value", "[],[" + result + "]");
					rowsMap.put(curRow.key(), newRow);
				}
			}
		}
		
		return new ArrayList<>(rowsMap.values());
	}

	private static List<Row> joinTables(Iterator<Row> firstRows, Iterator<Row> secondRows) {
		List<Row> newRows = new ArrayList<>();
		Map<String, Row> rowsMap  = new HashMap<>();
		Row curRow;
		
		// convert the elements of the current RDD to a HashSet
		while (firstRows.hasNext()) {
			curRow = firstRows.next();
			rowsMap.put(curRow.key(), curRow);
		}
		
		while (secondRows.hasNext()) {
			curRow = secondRows.next();
			if (rowsMap.containsKey(curRow.key())) {
				Row row = rowsMap.get(curRow.key());
				Row newRow = new Row(row.key());
				
				for (String col1 : row.columns()) {
					for (String col2 : curRow.columns()) {
						String val = combineValues(row.get(col1), curRow.get(col2));
						String uniqueCol = generateUniqueColName(col1, col2);
						newRow.put(uniqueCol, val);
						rowsMap.put(row.key(), newRow);
					}
				}
				
			} else {
				rowsMap.put(curRow.key(), curRow);
			}
		}
		
		return new ArrayList<>(rowsMap.values());
	}

	private static String generateUniqueColName(String col1, String col2) {
		
		return Hasher.hash(col1) + "_" + Hasher.hash(col2);
	}

	/**
	 * Gets group map. (EC for groupBy)
	 * @param rows
	 * @param lambda
	 * @return
	 */
	private static Map<String, List<String>> getGroupMap(Iterator<Row> rows, StringToString lambda) throws Exception {
		Map<String, List<String>> groupMap = new HashMap<>();
		
		while (rows.hasNext()) {
			Row row = rows.next();
			String element = row.get("value");
			String key = lambda.op(element);
			groupMap.computeIfAbsent(key, k -> new ArrayList<>()).add(element);
		}
		
		return groupMap;
	}

	/**
	 * Generates the sampled data based on the original data and probability f. (EC for sample)
	 * @param originalData
	 * @param f
	 * @return
	 */
	private static List<Row> generateSampleData(Iterator<Row> originalData, Double f) {
		if (f < 0.0 || f > 1.0) 
			throw new IllegalArgumentException("Probability must be between 0 and 1");
		
	    List<Row> sampledData = new ArrayList<>();
	
	    Random random = new Random();
	
	    while (originalData.hasNext()) {
	    	Row row = originalData.next();
	    	
	        if (random.nextDouble() <= f) {
	            sampledData.add(row);
	        }
	    }
	
	    return sampledData;
		
	}

	/**
	 * Finds the common keys between two tables. (EC for intersection)
	 * @param firstRows
	 * @param secondRows
	 * @return
	 */
	private static List<Row> findIntersection(Iterator<Row> firstRows, Iterator<Row> secondRows) {
		List<Row> commonRowsList = new ArrayList<>();
		Set<String> commonRowKeys = new HashSet<>(); 
		Set<String> firstRowsKeySet = new HashSet<>();
		Row curRow;
		
		// convert the elements of the current RDD to a HashSet
		while (firstRows.hasNext()) {
			curRow = firstRows.next();
			firstRowsKeySet.add(curRow.get("value"));
		}
		
		// for each element in the other RDD, check if it exists in the HashSet
		while (secondRows.hasNext()) {
			curRow = secondRows.next();
			String curElement = curRow.get("value");
			if (firstRowsKeySet.contains(curElement)) {
				commonRowsList.add(curRow);
				firstRowsKeySet.remove(curElement);
			}
		}
		
		return commonRowsList;
	}

	private static String generateUniqueRowKey(String key) {
		
		return key + "_" + UUID.randomUUID().toString();
	}
	
	private static String combineValues(String value1, String value2) {
	    // combine the values as needed (e.g., concatenate them)
	    return value1 + "," + value2;
	}
}
