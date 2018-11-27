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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class FlowcountReducer extends Reducer<Text,PhoneFlow,Text,PhoneFlow>{

  private PhoneFlow flow = new PhoneFlow();

  @Override
  protected void reduce(Text key, Iterable<PhoneFlow> values, Context context) throws IOException,
    InterruptedException {

    int up = 0;
    int down = 0;

    for (PhoneFlow value : values) {
      up += value.getUpFlow();
      down += value.getDownFlow();
    }
    flow.setUpFlow(up);
    flow.setDownFlow(down);
    flow.setPhone(key.toString());
    System.out.println("--->" + flow);
    context.write(key, flow);
  }
}
