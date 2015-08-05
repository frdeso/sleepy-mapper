package com.frdeso.app;
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.BufferedReader;
import java.io.IOException;
import java.lang.InterruptedException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.base.Charsets;

public class JobA extends Configured implements Tool {

  /**
   * Maps words from line of text into 2 key-value pairs; one key-value pair for
   * counting the word, another for counting its length.
   */
  public static class JobAMapper extends
      Mapper<Object, Text, Text, IntWritable> {

    private int sleepDuration;
    private final static IntWritable one = new IntWritable(1);
    /**
     * Emits 2 key-value pairs for counting the word and its length. Outputs are
     * (Text, LongWritable).
     * 
     * @param value
     *          This will be a line of text coming in from our input file.
     */
    public void map(Object key, Text value, Context context)
	throws  IOException,InterruptedException {
	
	Configuration conf = context.getConfiguration();
	sleepDuration = Integer.parseInt(conf.get("mapSleepTime"));
	try
	{
		Thread.sleep(sleepDuration * 1000);
	}
	catch(InterruptedException e)
	{
		System.out.println(e.getMessage());
	}
	context.write(value, one );	
      }
  }

  /**
   * Performs integer summation of all the values for each key.
   */


  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 3) {
      System.err.println("Usage: wordmean <in> <out> <number second>");
      return 0;
    }

    Configuration conf = getConf();
    conf.set("mapSleepTime", args[2]);
    @SuppressWarnings("deprecation")
    Job job = new Job(conf, "joba");
    job.setJarByClass(JobA.class);
    job.setMapperClass(JobAMapper.class);
    job.setCombinerClass(Reducer.class);
    job.setReducerClass(Reducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    Path outputpath = new Path(args[1]);
    FileOutputFormat.setOutputPath(job, outputpath);
    boolean result = job.waitForCompletion(true);

    return (result ? 0 : 1);
  }

  /**
   * Only valuable after run() called.
   * 
   * @return Returns the mean value.
   */
}
