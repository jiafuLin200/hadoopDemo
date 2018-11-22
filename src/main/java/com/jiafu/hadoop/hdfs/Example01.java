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
package com.jiafu.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class Example01 {
  private FileSystem fileSystem = null;

  @Before
  public void init() throws IOException, URISyntaxException, InterruptedException {
    Configuration conf = new Configuration();
    conf.set("fs.defaultFs", "hdfs://hdp01:9000");
    conf.set("dfs.namenode.fs-limits.min-block-size", "65535");
    conf.set("dfs.blocksize", "128k");

    fileSystem = FileSystem.get(new URI("hdfs://hdp01:9000"), conf, "root");
  }

  @Test
  public void Test_copyToLocal() throws IOException {

    Path srcPath = new Path("/test/vertex.txt");
    Path dstPath = new Path("/home/jiafu/Desktop/graphx/vertex2.txt");
    if (fileSystem.exists(srcPath)) {
      fileSystem.copyToLocalFile(srcPath, dstPath);
      System.out.println("拷贝成功");
    } else {
      System.out.println("拷贝失败");
    }
  }

  @Test
  public void Test_copyFromLocal() throws IOException {
    fileSystem.copyFromLocalFile(new Path(
      "/home/jiafu/Desktop/graphx/vertex.txt"), new Path("/test"));
  }

  @After
  public void close() throws IOException {
    if (fileSystem != null) {
      fileSystem.close();
    }
  }
}
