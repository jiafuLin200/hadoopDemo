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
package com.jiafu.hadoop.mapreduce.wordcount.example02;

import com.jiafu.hadoop.mapreduce.wordcount.example02.bean.PhoneFlow;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowcountMapper extends Mapper<LongWritable, Text, Text, PhoneFlow> {

  private PhoneFlow flow = new PhoneFlow();

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException,
    InterruptedException {
    String line = value.toString();
    String[] words = line.split("\t");

    //将单词输出(单词,1)
    flow.setPhone(words[0]);
    flow.setUpFlow(Integer.parseInt(words[1]));
    flow.setDownFlow(Integer.parseInt(words[2]));
    System.out.println(flow);
    context.write(new Text(words[0]), flow);
  }
}
