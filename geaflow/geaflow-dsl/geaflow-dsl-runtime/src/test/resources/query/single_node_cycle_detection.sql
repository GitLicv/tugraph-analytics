/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

-- 设置图算法的相关配置
SET geaflow.dsl.window.size = -1;

-- 定义顶点表
CREATE TABLE v_node (
  id bigint,
  label varchar
) WITH (
  type='file',
  geaflow.dsl.file.path = 'resource:///data/test_vertex.txt'
);

-- 定义边表
CREATE TABLE e_edge (
  srcId bigint,
  targetId bigint,
  type varchar
) WITH (
  type='file',
  geaflow.dsl.file.path = 'resource:///data/test_edge.txt'
);

-- 定义图结构
CREATE GRAPH test_graph (
  Vertex node using v_node WITH ID(id),
  Edge edge using e_edge WITH ID(srcId, targetId)
) WITH (
  storeType='rocksdb',
  shardCount = 2
);

-- 输出表
CREATE TABLE tbl_result (
  cycle_path string
) WITH (
  type='file',
  geaflow.dsl.file.path='${target}'
);

-- 指定使用图
USE GRAPH test_graph;

-- 调用算法：传入 6 个参数（必须）
-- 格式: sourceId, vertexType, edgeType, minCycleLength, maxCycleLength, limitCycleNum
INSERT INTO tbl_result
CALL single_node_cycle_detection(
    1,               -- sourceId
    'node',          -- vertex label/type，与你图结构中 Vertex node 对应
    'edge',          -- edge label/type，与你图结构中 Edge edge 对应
    2,               -- 最小圈长
    10,              -- 最大圈长
    100              -- 最多圈数量
)
YIELD (cycle_path);