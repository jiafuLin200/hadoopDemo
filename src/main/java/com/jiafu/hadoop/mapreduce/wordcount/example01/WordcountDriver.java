/*
 * Copyright © Since 2018 www.isinonet.com Company
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jiafu.hadoop.mapreduce.wordcount.example01;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 相当于yarn的客户端.
 * 封装mr程序相关参数,提交给yarn.
 */
public class WordcountDriver {

  public static void main(String[] args) throws IOException, ClassNotFoundException,
    InterruptedException {
    Configuration conf = new Configuration();
    //提交远程yarn执行
//    conf.set("mapreduce.framework.name","yarn");
//    conf.set("yarn.resourcemanager.hostname","hdp01");
//    conf.set("fs.defaultFS","hdfs://hdp01:9000/");
    //在本地线程执行
    conf.set("mapreduce.framework.name","local");
    conf.set("fs.defaultFS","file:///");

    Job job = Job.getInstance(conf);

    job.setJarByClass(WordcountDriver.class);

    //指定需要用到的类
    job.setMapperClass(WordcountMapper.class);
    job.setReducerClass(WordcountReducer.class);

    //指定mapper输出类型
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    //指定最终输出类型
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    //自定义分区
    job.setPartitionerClass(CustomPartitioner.class);
    job.setNumReduceTasks(5);

    //指定读取的数据目录
    FileInputFormat.setInputPaths(job,new Path(args[0]));
    //指定输出目录
    FileOutputFormat.setOutputPath(job,new Path(args[1]));

    //将job中配置的参数和使用的类提交给yarn
//    job.submit();
    boolean res = job.waitForCompletion(true);
    System.exit(res?0:-1);
  }

}
