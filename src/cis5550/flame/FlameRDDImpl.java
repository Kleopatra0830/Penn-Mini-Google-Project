package cis5550.flame;

import java.util.*;

import cis5550.flame.FlamePairRDD.TwoStringsToString;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Serializer;

public class FlameRDDImpl implements FlameRDD {
	private String tableName;
	private FlameContextImpl context;
	private boolean isDestroyed;

	public FlameRDDImpl(String tableName, String jarName) {
		this.tableName = tableName;
		this.context = new FlameContextImpl(jarName);
		this.isDestroyed = false;
	}
	
	/**
	 * @return the tableName
	 */
	public String getTableName() {
		return tableName;
	}

	@Override
	public List<String> collect() throws Exception {
		ensureNotDestroyed();
		Iterator<Row> rows = Coordinator.kvs.scan(tableName);
		List<String> res = new ArrayList<>();
		
		while (rows.hasNext()) {
			Row row = rows.next();
			res.add(row.get("value"));
		}
		
		return res;
	}

	@Override
	public FlameRDD flatMap(StringToIterable lambda) throws Exception {
		ensureNotDestroyed();
		// serialize the lambda
		byte[] serializedLambda = Serializer.objectToByteArray(lambda);
		// invoke the operation
		String outputTable = null;
		try {
			outputTable = context.invokeOperation("/rdd/flatMap", serializedLambda, tableName, null, null, null, false, null);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return new FlameRDDImpl(outputTable, context.getJarName());
	}

	@Override
	public FlamePairRDD mapToPair(StringToPair lambda) throws Exception {
		ensureNotDestroyed();
		// serialize the lambda
		byte[] serializedLambda = Serializer.objectToByteArray(lambda);
		// invoke the operation
		String outputTable = null;
		try {
			outputTable = context.invokeOperation("/rdd/mapToPair", serializedLambda, tableName, null, null, null, null, null);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return new FlamePairRDDImpl(outputTable, context.getJarName());
	}

	@Override
	public FlameRDD intersection(FlameRDD r) throws Exception { // EC intersection
		ensureNotDestroyed();
		// invoke the operation
		String outputTable = null;
		try {
			outputTable = context.invokeOperation("/rdd/intersection", null, tableName, null, ((FlameRDDImpl) r).getTableName(), null, null, null);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return new FlameRDDImpl(outputTable, context.getJarName());
	}

	@Override
	public FlameRDD sample(double f) throws Exception { // EC sampling
		ensureNotDestroyed();
		// invoke the operation
		String outputTable = null;
		try {
			outputTable = context.invokeOperation("/rdd/sample", null, tableName, null, null, f, null, null);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return new FlameRDDImpl(outputTable, context.getJarName());
	}

	@Override
	public FlamePairRDD groupBy(StringToString lambda) throws Exception { // EC groupBy
		ensureNotDestroyed();
		// serialize the lambda
		byte[] serializedLambda = Serializer.objectToByteArray(lambda);
		// invoke the operation
		String outputTable = null;
		try {
			outputTable = context.invokeOperation("/rdd/groupBy", serializedLambda, tableName, null, null, null, null, null);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return new FlamePairRDDImpl(outputTable, context.getJarName());
	}

	@Override
	public int count() throws Exception {
		ensureNotDestroyed();
		return Coordinator.kvs.count(tableName);
	}

	@Override
	public void saveAsTable(String tableNameArg) throws Exception {
		ensureNotDestroyed();
		Coordinator.kvs.rename(tableName, tableNameArg);
	}

	@Override
	public FlameRDD distinct() throws Exception {
		ensureNotDestroyed();
		// invoke the operation
		String outputTable = null;
		try {
			outputTable = context.invokeOperation("/rdd/distinct", null, tableName, null, null, null, null, null);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return new FlameRDDImpl(outputTable, context.getJarName());
	}

	@Override
	public void destroy() throws Exception {
		ensureNotDestroyed();
		Coordinator.kvs.delete(tableName);
		isDestroyed = true;
	}
	
	/**
	 * A utility method to check if the RDD is destroyed before any operation.
	 */
    private void ensureNotDestroyed() {
        if (isDestroyed) {
            throw new IllegalStateException("The RDD has been destroyed. Operations on this RDD are not allowed.");
        }
    }

	@Override
	public Vector<String> take(int num) throws Exception {
		ensureNotDestroyed();
		Vector<String> subSet = new Vector<>();
		
		Iterator<Row> rows = Coordinator.kvs.scan(tableName);
		
		while (rows.hasNext() && subSet.size() < num) {
			Row row = rows.next();
			subSet.add(row.get("value"));
		}
		
		return subSet;
	}

	@Override
	public String fold(String zeroElement, TwoStringsToString lambda) throws Exception {
		ensureNotDestroyed();
		// serialize the lambda
		byte[] serializedLambda = Serializer.objectToByteArray(lambda);
		// invoke the operation
		String outputTable = null;
		List<String> responses = new ArrayList<>();
		try {
			outputTable = context.invokeOperation("/rdd/fold", serializedLambda, tableName, zeroElement, null, null, null, responses);
		} catch (Exception e) {
			e.printStackTrace();
		}
		String ans = responses.get(0);
		
		// fold responses
		for (int i = 1; i < responses.size(); i++) {
			ans = lambda.op(ans, responses.get(i));
		}
		
		return ans;
	}

	@Override
	public FlamePairRDD flatMapToPair(StringToPairIterable lambda) throws Exception {
		ensureNotDestroyed();
		// serialize the lambda
		byte[] serializedLambda = Serializer.objectToByteArray(lambda);
		// invoke the operation
		String outputTable = null;
		try {
			outputTable = context.invokeOperation("/rdd/flatMapToPair", serializedLambda, tableName, null, null, null, false, null);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return new FlamePairRDDImpl(outputTable, context.getJarName());
	}

	@Override
	public FlameRDD filter(StringToBoolean lambda) throws Exception {
		ensureNotDestroyed();
		// serialize the lambda
		byte[] serializedLambda = Serializer.objectToByteArray(lambda);
		// invoke the operation
		String outputTable = null;
		try {
			outputTable = context.invokeOperation("/rdd/filter", serializedLambda, tableName, null, null, null, null, null);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return new FlameRDDImpl(outputTable, context.getJarName());
	}

	@Override
	public FlameRDD mapPartitions(IteratorToIterator lambda) throws Exception {
		ensureNotDestroyed();
		// serialize the lambda
		byte[] serializedLambda = Serializer.objectToByteArray(lambda);
		// invoke the operation
		String outputTable = null;
		try {
			outputTable = context.invokeOperation("/rdd/mapPartitions", serializedLambda, tableName, null, null, null, null, null);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return new FlameRDDImpl(outputTable, context.getJarName());
	}
}
