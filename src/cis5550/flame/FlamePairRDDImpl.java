package cis5550.flame;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import cis5550.kvs.Row;
import cis5550.tools.Serializer;

public class FlamePairRDDImpl implements FlamePairRDD {	
	private String tableName;
	private FlameContextImpl context;
	private boolean isDestroyed;

	public FlamePairRDDImpl(String tableName, String jarName) {
		this.tableName = tableName;
		this.context = new FlameContextImpl(jarName);
		this.isDestroyed = false;
	}

	@Override
	public List<FlamePair> collect() throws Exception {
		ensureNotDestroyed();
		Iterator<Row> rows = Coordinator.kvs.scan(tableName);
		List<FlamePair> res = new ArrayList<>();
		
		while (rows.hasNext()) {
			Row row = rows.next();
			for (String column : row.columns()) {
				res.add(new FlamePair(row.key(), row.get(column)));
			}
		}
		
		return res;
	}

	@Override
	public FlamePairRDD foldByKey(String zeroElement, TwoStringsToString lambda) throws Exception {
		ensureNotDestroyed();
		// serialize the lambda
		byte[] serializedLambda = Serializer.objectToByteArray(lambda);
		// invoke the operation
		String outputTable = null;
		try {
			outputTable = context.invokeOperation("/rdd/foldByKey", serializedLambda, tableName, zeroElement, null, null, null, null);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return new FlamePairRDDImpl(outputTable, context.getJarName());
	}

	@Override
	public void saveAsTable(String tableNameArg) throws Exception {
		ensureNotDestroyed();
		Coordinator.kvs.rename(tableName, tableNameArg);	
	}

	@Override
	public FlameRDD flatMap(PairToStringIterable lambda) throws Exception {
		ensureNotDestroyed();
		// serialize the lambda
		byte[] serializedLambda = Serializer.objectToByteArray(lambda);
		// invoke the operation
		String outputTable = null;
		try {
			outputTable = context.invokeOperation("/rdd/flatMap", serializedLambda, tableName, null, null, null, true, null);
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
	public FlamePairRDD flatMapToPair(PairToPairIterable lambda) throws Exception {
		ensureNotDestroyed();
		// serialize the lambda
		byte[] serializedLambda = Serializer.objectToByteArray(lambda);
		// invoke the operation
		String outputTable = null;
		try {
			outputTable = context.invokeOperation("/rdd/flatMapToPair", serializedLambda, tableName, null, null, null, true, null);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return new FlamePairRDDImpl(outputTable, context.getJarName());
	}

	@Override
	public FlamePairRDD join(FlamePairRDD other) throws Exception {
		ensureNotDestroyed();
		// invoke the operation
		String outputTable = null;
		try {
			outputTable = context.invokeOperation("/rdd/join", null, tableName, null, ((FlamePairRDDImpl) other).getTableName(), null, true, null);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return new FlamePairRDDImpl(outputTable, context.getJarName());
	}

	private String getTableName() {

		return tableName;
	}

	@Override
	public FlamePairRDD cogroup(FlamePairRDD other) throws Exception {
		ensureNotDestroyed();
		// invoke the operation
		String outputTable = null;
		try {
			outputTable = context.invokeOperation("/rdd/cogroup", null, tableName, null, ((FlamePairRDDImpl) other).getTableName(), null, true, null);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return new FlamePairRDDImpl(outputTable, context.getJarName());
	}

}
