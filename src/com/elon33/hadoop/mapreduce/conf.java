package com.elon33.hadoop.mapreduce;


import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.is;

public class conf {

	@Test
	public void test(){
		Configuration conf = new Configuration();
		conf.addResource("configuration.xml");
		System.setProperty("weight", "weight");
		assertThat(conf.get("weight"), is("light"));
	}
}
