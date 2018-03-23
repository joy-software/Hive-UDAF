package com.joy.example.udaf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.ql.metadata.HiveException;


/**
 * The aim of this class is to calculate the mean on a col
 * @author ZVJN1964
 *Syntax: ... MeanUdaf(col) 
 */
public class MeanUdaf extends UDAF {

	public static class MeanUDAFEvaluator implements UDAFEvaluator {
		/**
		 * Use Column class to serialize intermediate computation
		 * This is our groupByColumn
		 */
		public static class Result {
			double sum = 0;
			int count = 0;
		}

		private Result result = null;

		public MeanUDAFEvaluator() {
			super();
			init();
		}


		//Initalize evaluator - indicating that no values have been
		// aggregated yet.
		public void init() {
			result = new Result();
		}

		// called each time a new value is aggregated
		public boolean iterate(double value) throws HiveException {

			if (result == null)
				throw new HiveException("Item is not initialized");
			result.sum = result.sum + value;
			result.count = result.count + 1;
			return true;
		}

		// Called when Hive wants partially aggregated results.
		public Result terminatePartial() {

			return result;
		}
		
		// Called when Hive decides to combine one partial aggregation with the current
		public boolean merge(Result other) {

			if(other == null) {
				return true;
			}
			result.sum += other.sum;
			result.count += other.count;
			return true; 
		}
		
		//Give the final result of the aggregation.
		public double terminate(){

			return result.sum/result.count;
		}
	}


}
