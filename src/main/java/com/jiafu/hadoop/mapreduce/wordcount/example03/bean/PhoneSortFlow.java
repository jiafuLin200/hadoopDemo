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
package com.jiafu.hadoop.mapreduce.wordcount.example03.bean;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PhoneSortFlow implements WritableComparable<PhoneSortFlow> {
  private String phone;
  private int upFlow;
  private int downFlow;

  public String getPhone() {
    return phone;
  }

  public int getUpFlow() {
    return upFlow;
  }

  public int getDownFlow() {
    return downFlow;
  }

  public void setPhone(String phone) {
    this.phone = phone;
  }

  public void setUpFlow(int upFlow) {
    this.upFlow = upFlow;
  }

  public void setDownFlow(int downFlow) {
    this.downFlow = downFlow;
  }

  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeUTF(phone);
    dataOutput.writeInt(upFlow);
    dataOutput.writeInt(downFlow);
  }

  public void readFields(DataInput dataInput) throws IOException {
    phone = dataInput.readUTF();
    upFlow = dataInput.readInt();
    downFlow = dataInput.readInt();
  }

  @Override
  public String toString() {
    return phone + "\t" + upFlow + "\t" + downFlow;
  }

  public int compareTo(PhoneSortFlow o) {
    if (this.upFlow > o.upFlow) {
      return 1;
    } else if (this.upFlow == o.upFlow && this.downFlow > o.downFlow) {
      return 1;
    }

    return -1;
  }
}
