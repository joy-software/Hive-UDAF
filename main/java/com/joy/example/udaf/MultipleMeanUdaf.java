package com.joy.example.udaf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

/**
 * Calculate the mean over multiple column
 * @author ZVJN1964
 *syntax: ...MutipleMeanUdaf(col1, col2, ... , coln)
 */
public class MultipleMeanUdaf extends AbstractGenericUDAFResolver {

	private static int length = 0;
	
	public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws UDFArgumentTypeException
	{

		length = parameters.length;

		//We verify that we have at least one parameters
		assert(length > 0);

		//We have to inspect the Object pass through the function
		//We create an array of inspector since we may have many parameters
		ObjectInspector[] oi =  new ObjectInspector[length];

		//Get the objectInspector of our parameters
		for(int i = 0; i < length; i++)
		{
			oi[i] = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(parameters[i]);
		}

		//Verify that all those parameters are primitives and check if we can compute a mean on it
		for(ObjectInspector inspector : oi)
		{
			if(inspector.getCategory() != ObjectInspector.Category.PRIMITIVE)
			{
				throw new UDFArgumentTypeException(0,
						"Argument must be PRIMITIVE, but "
								+ inspector.getCategory().name()
								+ " was passed.");
			}

			PrimitiveObjectInspector pInspector = (PrimitiveObjectInspector)inspector;

			//check if we can compute a mean on the data we have received
			//Only double, float and int are accepted
			switch(pInspector.getPrimitiveCategory())
			{
			case DOUBLE:
				break;
			case FLOAT:
				break;
			case INT:
				break;
			default:
				throw new UDFArgumentTypeException(0,
						"Argument must be double, float or int, but "
								+ pInspector.getPrimitiveCategory()
								+ " was passed.");
			}

			//Perform the verification
			for (int i = 0; i < length; i++) {
				if(i != 0)
				{
					if (!oi[i].getTypeName().equals(oi[i - 1].getTypeName())) {
						throw new UDFArgumentTypeException(i, "Argument type \""
								+ oi[i].getTypeName()
								+ "\" is different from preceding arguments. "
								+ "Previous type was \"" + oi[i - 1].getTypeName() + "\"");
					}
				}
			}

		}


		//We call our evaluator
		return new MultipleMeanUdafEvaluator();

	}

	//Our class evaluator, this class will contains all the UDAF behavior
	/**
	 * 
	 * @author ZVJN1964
	 *Remember that to manipulate data during a Hive execution, we need some objectInspector to 
	 *catch all the informations on their concerns and have a hand on.
	 */
	public static class MultipleMeanUdafEvaluator extends GenericUDAFEvaluator
	{

		// Global variables that inspect the input data.
		//ObjectInspector is the general inspector but to be at ease when manipulating your data
		//Hive recommended to specialize your Inspector using one that was build your dataType
		//e.g: ListObjectInspector for List, StructInspector for Struct
		//The different inspectors that you could use can be find 
		//here: https://hive.apache.org/javadocs/r1.2.2/api/org/apache/hadoop/hive/serde2/objectinspector/package-summary.html
		// These are set up during the init() call, and are then used during the
		// calls of the other methods

		// ObjectInspector for the list it will be the input in partial 2 or final mode
		private ListObjectInspector loi;
		//The inspector that will be used for the data for partial 1 and complete mode
		private PrimitiveObjectInspector poi;
		//The length of our original data
		


		public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException
		{
			super.init(m, parameters);

			/**
			 * PARTIAL1: from original data to partial aggregation data: iterate() and
			 * terminatePartial() will be called.
			 */
			/**
			 * PARTIAL2: from partial aggregation data to partial aggregation data:
			 * merge() and terminatePartial() will be called.
			 */
			/**
			 * FINAL: from partial aggregation to full aggregation: merge() and
			 * terminate() will be called.
			 */
			/**
			 * COMPLETE: from original data directly to full aggregation: iterate() and
			 * terminate() will be called.
			 */
			//We have received the original data
			if(m == Mode.PARTIAL1 || m == Mode.COMPLETE)
			{	
				//Ensure that we have at least one parameter
				assert(length > 0);
				//We get our ObjectInspector, since all our data are from the same type
				//We can just get the first one, to catch the good Inspector for each value
				poi = (PrimitiveObjectInspector)parameters[0];

			}
			else
			{
				//We must have a list as parameter, only one list.
				assert(parameters.length == 1);
				loi = ObjectInspectorFactory.getStandardListObjectInspector(parameters[0]);
			}

			//The result will be inside a list [mean(col1), mean(col2), ..., mean(coln)]
			return ObjectInspectorFactory.getStandardListObjectInspector(parameters[0]);

		}


		/**
		 * class for storing the values received from Hive
		 */
		static class MeanAgg implements AggregationBuffer
		{
			Double[] sum ;
		}

		/**
		 * This method will return a new MeanAgg aggregationBuffer
		 */
		@Override
		public AggregationBuffer getNewAggregationBuffer() throws HiveException {
			MeanAgg agg = new MeanAgg();
			reset(agg);
			return agg;
		}

		//This method will empty the data inside our agg Buffer
		@Override
		public void reset(AggregationBuffer agg) throws HiveException {
			//The last double will store the counter
			((MeanAgg)agg).sum =  new Double[length+1];
			for(int i = 0;  i < length+1; i++  )
			{
				((MeanAgg)agg).sum[i] = 0.0;
			}
		}

		//iterate is called each our aggragator receive a new row value
		//iterate is called in partial 1 or complete mode, 
		//So we have our original data as parameters
		@Override
		public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {

			//assert(parameters.length > 0);

			MeanAgg mAgg = (MeanAgg)agg;

			for(int i = 0;  i < parameters.length; i++ )
			{
				Object object = parameters[i];

				if(object != null)
				{
						mAgg.sum[i] += Double.parseDouble(""+poi.getPrimitiveJavaObject(object));
				} 
			}

			//increment the counter
			mAgg.sum[length]++;
		}

		//This method is called and the end of a partial task
		@Override
		public Object terminatePartial(AggregationBuffer agg) throws HiveException {
			return ((MeanAgg)agg).sum;
		}

		//The aim of this method is to compute a partial result with the current aggregation
		@Override
		public void merge(AggregationBuffer agg, Object partial) throws HiveException {

			if(partial != null)
			{
				Double[] data = (Double[])partial;

				MeanAgg current = (MeanAgg)agg;

				for(int i = 0; i < length; i++)
				{
					current.sum[i] += data[i];
				}
				
				current.sum[length] += data[length];
			}

		}

		//The aim of this method is to return the final result to the user
		@Override
		public Object terminate(AggregationBuffer agg) throws HiveException {
			// TODO Auto-generated method stub
			Double[] result = new Double[length] ;
			
			MeanAgg mAgg = (MeanAgg)agg;

			//Mean calculation, the result must not contain the counter as the last item
			for(int i = 0; i < length; i++)
			{
				result[i] = mAgg.sum[i] / mAgg.sum[length];
			}

			return result;
		}

	}

}
