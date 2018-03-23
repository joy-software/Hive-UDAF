package com.joy.example.udaf;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.junit.Assert;
import org.junit.Test;

import com.joy.example.udaf.MeanUdaf.MeanUDAFEvaluator;

public class MeanUdafTest {
	
	@Test 
	public void testMeanCalcultation() throws HiveException
	{
		
		MeanUdaf.MeanUDAFEvaluator mean1 = new MeanUDAFEvaluator();
		MeanUdaf.MeanUDAFEvaluator mean2 = new MeanUDAFEvaluator();
		
		
		//initialization of our variables
		mean1.init();
		mean2.init();
		
		//Perform some iterations
		Assert.assertEquals(mean1.iterate(2),true);
		Assert.assertEquals(mean1.iterate(4),true);
		Assert.assertEquals(mean2.iterate(4),true);
		Assert.assertEquals(mean2.iterate(6),true);
		
		//Perform Merge
		Assert.assertEquals(mean1.merge(mean2.terminatePartial()), true);
		
		//Test the calcultion
		Assert.assertEquals("Error", 4, mean1.terminate(), 0);
		
		
	}

}
