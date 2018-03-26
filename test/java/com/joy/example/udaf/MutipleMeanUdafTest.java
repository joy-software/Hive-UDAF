package com.joy.example.udaf;

import static org.junit.Assert.assertArrayEquals;

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.joy.example.udaf.MultipleMeanUdaf.MultipleMeanUdafEvaluator.MeanAgg;

import junit.framework.Assert;

public class MutipleMeanUdafTest {
	
	
	MultipleMeanUdaf mean;
    GenericUDAFEvaluator evaluator;
    private ObjectInspector[] output;
	private PrimitiveObjectInspector[] poi;

    MultipleMeanUdaf.MultipleMeanUdafEvaluator.MeanAgg agg;

 
    Object[] param1 = {2.0, 4.0, 7.9, 2.0};
    Object[] param2 = {4.0, 2.0, 2.1, 8.0};
    Object[] param3 = {8.0, 8.0, 2.1, 4.0};
    Object[] param4 = {2.0, 2.0, 3.9, 2.0};
    
    Object[] error11 = {2, 2.0, 4, 2};

    
    @Before
    public void setUp() throws Exception {
    	
    	mean = new MultipleMeanUdaf();
    	
    	//All the data are double
    	String[] typeStrs = {"double", "double", "double", "double"};
        TypeInfo[] types = makePrimitiveTypeInfoArray(typeStrs);
        
        evaluator = mean.getEvaluator(types);
        
        poi = new PrimitiveObjectInspector[4];
        poi[0] =  PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
                PrimitiveObjectInspector.PrimitiveCategory.DOUBLE);
        poi[1] =  PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
                PrimitiveObjectInspector.PrimitiveCategory.DOUBLE);
        poi[2] =  PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
                PrimitiveObjectInspector.PrimitiveCategory.DOUBLE);
        poi[3] =  PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
                PrimitiveObjectInspector.PrimitiveCategory.DOUBLE);
        
        //The output inspector
    	output = new ObjectInspector[1];
    	
    	output[0] = ObjectInspectorFactory.getStandardListObjectInspector(poi[0]);
    	
    	agg = (MeanAgg) evaluator.getNewAggregationBuffer();
    }
    
    @After
    public void tearDown() throws Exception {

    }
    
    @Test(expected = UDFArgumentTypeException.class)
    public void testGetEvaluateorWithComplexTypes() throws Exception {
        TypeInfo[] types = new TypeInfo[1];
        types[0] = TypeInfoFactory.getListTypeInfo(TypeInfoFactory.getPrimitiveTypeInfo("int"));
        mean.getEvaluator(types);
    }
    
    @Test(expected = UDFArgumentTypeException.class)
    public void testGetEvaluateorWithNotSupportedTypes() throws Exception {
        TypeInfo[] types = new TypeInfo[1];
        types[0] = TypeInfoFactory.getPrimitiveTypeInfo("boolean");
        mean.getEvaluator(types);
    }
    
    @Test(expected = UDFArgumentTypeException.class)
    public void testGetEvaluateorWithString() throws Exception {
    	String[] typeStrs3 = {"double", "int", "string", "int"};
        TypeInfo[] types3 = makePrimitiveTypeInfoArray(typeStrs3);
        mean.getEvaluator(types3);
    }
    
    @Test(expected = UDFArgumentTypeException.class)
    public void testGetEvaluateWithMixingTypes() throws Exception {
    	
    	String[] typeStrs = {"double", "double", "int", "double"};
        TypeInfo[] types = makePrimitiveTypeInfoArray(typeStrs);
        mean.getEvaluator(types);
    }
    
    @Test
    public void testIterate() throws HiveException
    {
    	evaluator.init(GenericUDAFEvaluator.Mode.PARTIAL1, poi);
        evaluator.reset(agg);
        
        evaluator.iterate(agg,param1);
        //As a remind, our counter is at last position of sum array
        //Since we are passing an array of length 4, our counter is
        //at position 4
        Assert.assertEquals(1.0,agg.sum[4]);
        Double[] result1 = {2.0, 4.0, 7.9, 2.0,1.0};
        assertArrayEquals(agg.sum, result1);
        
        evaluator.iterate(agg,param2);
        Double[] result2 = {6.0, 6.0, 10.0, 10.0,2.0};
        assertArrayEquals(agg.sum, result2);
        
        evaluator.iterate(agg,param3);
        Double[] result3 = {14.0, 14.0, 12.1, 14.0,3.0};
        assertArrayEquals(agg.sum, result3);
        
        evaluator.iterate(agg,param4);
        Double[] result4 = {16.0, 16.0, 16.0, 16.0,4.0};
        assertArrayEquals(agg.sum, result4);
    }
    
    @Test
    public void testTerminatePartial() throws Exception {
        
    	testIterate();

        Object partial = evaluator.terminatePartial(agg);

        Assert.assertTrue(partial instanceof Double[]);
        Double[] result = {16.0, 16.0, 16.0, 16.0, 4.0};
        assertArrayEquals((Double[])partial, result);
    }

    @Test
    public void testMerge() throws Exception {
    	evaluator.init(GenericUDAFEvaluator.Mode.PARTIAL1, poi);
        evaluator.reset(agg);
        
        evaluator.iterate(agg,param1);        
        evaluator.iterate(agg,param2);

        Object partial1 = evaluator.terminatePartial(agg);
        
        evaluator.init(GenericUDAFEvaluator.Mode.PARTIAL1, poi);
        evaluator.reset(agg);
        evaluator.iterate(agg,param3);
        Object partial2 = evaluator.terminatePartial(agg);

        evaluator.init(GenericUDAFEvaluator.Mode.PARTIAL1, poi);
        evaluator.reset(agg);
        evaluator.iterate(agg, param4);
        Object partial3 = evaluator.terminatePartial(agg);

        evaluator.init(GenericUDAFEvaluator.Mode.PARTIAL2, output);
        evaluator.reset(agg);

        evaluator.merge(agg, partial1);
        //As a remind, our counter is at last position of sum array
        //Since we are passing an array of length 4, our counter is
        //at position 4
        Double[] result = {6.0, 6.0, 10.0, 10.0,2.0};
        assertArrayEquals(result,agg.sum);

        evaluator.merge(agg, partial2);
        Double[] result1 = {14.0, 14.0, 12.1, 14.0,3.0};
        assertArrayEquals(result1, agg.sum);
        
        evaluator.merge(agg, partial3);
        Double[] result2 = {16.0, 16.0, 16.0, 16.0,4.0};
        assertArrayEquals(result2, agg.sum);
    }
    
    @Test
    public void testTerminate() throws Exception {
        evaluator.init(GenericUDAFEvaluator.Mode.COMPLETE, poi);
        evaluator.reset(agg);

        evaluator.iterate(agg, param1);
        evaluator.iterate(agg, param2);
        evaluator.iterate(agg, param3);
        evaluator.iterate(agg, param4);
        Object term = evaluator.terminate(agg);

        Double[] result = {4.0, 4.0, 4.0, 4.0};
        
        Assert.assertTrue(term instanceof Double[]);
        assertArrayEquals((Double[])term, result);
    }
    
    
    /**
     * Generate some TypeInfo from the typeStrs
     * @param typeStrs
     * @return
     */
    private TypeInfo[] makePrimitiveTypeInfoArray(String[] typeStrs) {
        int len = typeStrs.length;

        TypeInfo[] types = new TypeInfo[len];

        for (int i = 0; i < len; i++) {
            types[i] = TypeInfoFactory.getPrimitiveTypeInfo(typeStrs[i]);
        }

        return types;
    }


}
