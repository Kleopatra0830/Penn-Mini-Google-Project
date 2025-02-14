package cis5550.flame;

import java.util.*;

import cis5550.kvs.KVSClient;
import cis5550.tools.*;
import cis5550.tools.Partitioner.Partition;
import cis5550.tools.HTTP.Response;
import cis5550.flame.*;
import java.io.Serializable;

public class FlameContextImpl implements FlameContext, Serializable {
	private final Map<String, Object> extension = new HashMap<>();
	private static final long serialVersionUID = 1L; // recommended for Serializable classes
	private String jarName;
	private String outputMessage;
	private long jobStartTime;
	private int sequenceNumber = 0;
//	private final Partitioner partitioner = new Partitioner();
//	private List<Worker> flameWorkers;
//	private static final KVSClient KVS = Coordinator.kvs;
	
	// Use transient for non-serializable or non-essential fields for serialization
    private transient final Partitioner partitioner = new Partitioner();
    private transient List<Worker> flameWorkers;
    private transient static final KVSClient KVS = Coordinator.kvs;
	
	public FlameContextImpl(String jarName) {
		this.jarName = jarName;
		this.jobStartTime = System.currentTimeMillis();
		this.flameWorkers = new ArrayList<>();
	}

	@Override
	public KVSClient getKVS() {
		return KVS;
	}

	@Override
	public void output(String s) {
		StringBuilder sb = new StringBuilder();
		if (outputMessage != null)
			sb.append(outputMessage);
		sb.append(s);
		outputMessage = sb.toString();
	}

	@Override
	public FlameRDD parallelize(List<String> list) throws Exception {
		sequenceNumber++;
		String tableName = generateFreshTableName();
		
		for (int i = 0; i < list.size(); i++) {
			String key = Hasher.hash(Integer.toString(i));
			KVS.put(tableName, key, "value", list.get(i));
		}
		
		FlameRDDImpl flameRdd = new FlameRDDImpl(tableName, jarName);
		
		return flameRdd;
	}
	
	/**
	 * Gets the output message.
	 * @return
	 */
	public Object getOutput() {
		return outputMessage == null ? "There was no output" : outputMessage;
	}
	
	/**
	 * Generates a fresh table name.
	 * @return
	 */
	private String generateFreshTableName() {
		return "job_" + jobStartTime + "_" + sequenceNumber;
	}
	
	/**
	 * A generic function for RDD.
	 * @param operation
	 */
	public String invokeOperation(String operation, byte[] serializedLambda, String inputTable, String zeroElement,
			String anotherTable, Double f, Boolean pairRDD, List<String> responses) throws Exception {
		String outputTable = generateFreshTableName();
		
		int workerNumbers = KVS.numWorkers();
//		System.out.println(workerNumbers);
//		System.out.println(operation);
		
		for (int i = 0; i < workerNumbers; i++) { // register KVS workers
			String startRange = KVS.getWorkerID(i);
			String endRange = getNextWorkerId(i);
			partitioner.addKVSWorker(KVS.getWorkerAddress(i), startRange, endRange);
			if (endRange == null) { // call addKVSWorker another time for key under the lowest ID
				partitioner.addKVSWorker(KVS.getWorkerAddress(i), null, KVS.getWorkerID(0));
			}
		}
		
		for (String worker : Coordinator.getWorkers()) { // register Flame workers
			partitioner.addFlameWorker(worker);
		}
		
		Vector<Partition> partitions = partitioner.assignPartitions();
		Vector<Thread> requestThreads = new Vector<>();
		Vector<Integer> statusCodes = new Vector<>();
		
		if (partitions == null) return null;
		
		for (Partition partition : partitions) {
			Thread t = new Thread(() -> {
				String workerAddress = partition.assignedFlameWorker;
				String fromKey = partition.fromKey;
				String toKeyExclusive = partition.toKeyExclusive;
				StringBuilder requestURLSb = new StringBuilder();
				try {
					requestURLSb.append("http://" + workerAddress + operation + "?inputTable="
							+ java.net.URLEncoder.encode(inputTable, "UTF-8") + "&outputTable=" 
							+ java.net.URLEncoder.encode(outputTable, "UTF-8"));
					String kvsCoordinator = KVS.getCoordinator();
					if (kvsCoordinator != null) {
						requestURLSb.append("&kvsCoordinator=" + kvsCoordinator);
					}
//					if (jarName != null) {
//						requestURLSb.append("&jarName=" + java.net.URLEncoder.encode(jarName, "UTF-8"));
//					}
					if (fromKey != null) {
						requestURLSb.append("&fromKey=" + fromKey);
					}
					if (toKeyExclusive != null) {
						requestURLSb.append("&toKeyExclusive=" + toKeyExclusive);
					}
					if ((operation.equals("/rdd/foldByKey") || operation.equals("/rdd/fold")) && zeroElement != null) {
						requestURLSb.append("&zeroElement=" + java.net.URLEncoder.encode(zeroElement, "UTF-8"));
					}
					if ((operation.equals("/rdd/intersection") 
							|| operation.equals("/rdd/join") 
							|| operation.equals("/rdd/cogroup")) 
							&& anotherTable != null) { // EC intersection
						requestURLSb.append("&anotherTable=" + java.net.URLEncoder.encode(anotherTable, "UTF-8"));
					}
					if (operation.equals("/rdd/sample") && f != null) { // EC sample
						requestURLSb.append("&propability=" + java.net.URLEncoder.encode(String.valueOf(f), "UTF-8"));
					}
					if ((operation.equals("/rdd/flatMap") || operation.equals("/rdd/flatMapToPair")) && pairRDD != null) {
						if (pairRDD) {
							requestURLSb.append("&pairRDD=true");
						} else {
							requestURLSb.append("&pairRDD=false");
						}
					}
					String requestURL = requestURLSb.toString();
					Response response = HTTP.doRequest("POST", requestURL, serializedLambda);
					if (responses != null)
						responses.add(new String(response.body(), "UTF-8"));
					int statusCode = response.statusCode();
					statusCodes.add(statusCode);
				} catch (Exception e) {
					e.printStackTrace();
				}
			});
			requestThreads.add(t);
			t.start();
		}
		
		for (Thread t : requestThreads) {
			t.join();
		}
		
		for (Integer statusCode : statusCodes) {
		    if (statusCode != 200) { // report failure to the caller
		    	throw new Exception("Requests failed or returned status codes other than 200"+ ": " +statusCode);
		    }
		}
		
		return outputTable;
	}
	
	/**
	 * @return the jarName
	 */
	public String getJarName() {
		return jarName;
	}

	/**
	 * Gets the ID of the next worker or return null if the current worker is the last one.
	 * @param idx
	 * @return
	 */
	private String getNextWorkerId(int idx) throws Exception {
		if (idx == KVS.numWorkers() - 1) {
			return null;
		} else {
			return KVS.getWorkerID(idx + 1);
		}
    }

	@Override
	public FlameRDD fromTable(String tableName, RowToString lambda) throws Exception {
		// serialize the lambda
		byte[] serializedLambda = Serializer.objectToByteArray(lambda);
		// invoke the operation
		String outputTable = null;
		try {
			outputTable = invokeOperation("/rdd/fromTable", serializedLambda, tableName, null, null, null, null, null);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return new FlameRDDImpl(outputTable, jarName);
	}

	@Override
	public void setConcurrencyLevel(int keyRangesPerWorker) {
		partitioner.setKeyRangesPerWorker(keyRangesPerWorker);
	}

	@Override
	public void addExtention(String key, Object o) {
		extension.put(key, o);
	}

	@Override
	public Object getExtention(String key) {
		try {
			return extension.get(key);
		} catch (Exception e) {
			throw new RuntimeException("No such key in FlameContext extension");
		}
    }

}
