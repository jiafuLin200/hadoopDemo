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

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义输出分区
 */
public class CustomPartitioner extends Partitioner<Text, IntWritable> {
  public int getPartition(Text key, IntWritable value, int numPartitions) {

    String s= key.toString();
    if(s.length() == 0)
      return 4;

    char c = s.charAt(0);
    if (c <= 'O') {
      return 0;
    } else if (c <= 'Z') {
      return 1;
    } else if (c <= 'o') {
      return 2;
    } else if (c <= 'z') {
      return 3;
    } else {
      return 4;
    }
  }

}
