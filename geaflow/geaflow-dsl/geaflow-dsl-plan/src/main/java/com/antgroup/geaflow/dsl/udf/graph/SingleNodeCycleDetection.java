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

package com.antgroup.geaflow.dsl.udf.graph;

import com.antgroup.geaflow.common.type.primitive.IntegerType;
import com.antgroup.geaflow.dsl.common.algo.AlgorithmRuntimeContext;
import com.antgroup.geaflow.dsl.common.algo.AlgorithmUserFunction;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.data.RowEdge;
import com.antgroup.geaflow.dsl.common.data.RowVertex;
import com.antgroup.geaflow.dsl.common.data.impl.ObjectRow;
import com.antgroup.geaflow.dsl.common.function.Description;
import com.antgroup.geaflow.dsl.common.types.GraphSchema;
import com.antgroup.geaflow.dsl.common.types.StructType;
import com.antgroup.geaflow.dsl.common.types.TableField;
import com.antgroup.geaflow.model.graph.edge.EdgeDirection;
import com.antgroup.geaflow.dsl.common.types.ArrayType;
import com.antgroup.geaflow.common.type.primitive.IntegerType;
import com.antgroup.geaflow.common.type.primitive.StringType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@Description(name = "single_node_cycle_detection", 
             description = "Detect cycles starting from a single node")
public class SingleNodeCycleDetection implements AlgorithmUserFunction<Object, Integer> {

    private AlgorithmRuntimeContext<Object, Integer> context;
    private Object sourceId;
    private Set<Object> visited;
    private List<List<Object>> cycles;
    private Map<Object, List<Object>> adjacencyList;

    @Override
    public void init(AlgorithmRuntimeContext<Object, Integer> context, Object[] parameters) {
        this.context = context;
        if (parameters.length < 1) {
            throw new IllegalArgumentException("Source ID parameter is required");
        }
        this.sourceId = parameters[0];
        this.visited = new HashSet<>();
        this.cycles = new ArrayList<>();
        this.adjacencyList = new HashMap<>();
    }

    @Override
    public void process(RowVertex vertex, Optional<Row> message, Iterator<Integer> messageIterator) {
        Object vertexId = vertex.getId();
        adjacencyList.putIfAbsent(vertexId, new ArrayList<>());
        Iterator<RowEdge> edgeIterator = context.loadEdges(EdgeDirection.OUT).iterator();
        List<Object> targets = adjacencyList.get(vertexId);
        while (edgeIterator.hasNext()) {
            RowEdge edge = edgeIterator.next();
            if (edge.getSrcId().equals(vertexId)) {
                targets.add(edge.getTargetId());
            }
        }
    }

    @Override
    public void finish(RowVertex vertex, Optional<Row> message) {
        // No special finish logic needed for this algorithm
    }

    public boolean iterate(long iteration) {  // Changed return type to boolean
        if (iteration == 1) {
            // Start DFS from source node to detect cycles
            dfs(sourceId, new ArrayList<>(), new HashSet<>());
            
            // Emit results
            for (List<Object> cycle : cycles) {
                context.take(ObjectRow.create(cycle));
            }
            return false; // Return false to terminate iteration
        }
        return false;
    }

    private void dfs(Object current, List<Object> path, Set<Object> currentPath) {
        if (currentPath.contains(current)) {
            // Found a cycle
            int index = path.indexOf(current);
            List<Object> cycle = new ArrayList<>(path.subList(index, path.size()));
            cycle.add(current); // Complete the cycle
            cycles.add(cycle);
            return;
        }

        if (visited.contains(current)) {
            return;
        }

        visited.add(current);
        currentPath.add(current);
        path.add(current);

        // Visit all neighbors
        if (adjacencyList.containsKey(current)) {
            for (Object neighbor : adjacencyList.get(current)) {
                dfs(neighbor, path, currentPath);
            }
        }

        path.remove(path.size() - 1);
        currentPath.remove(current);
    }

    @Override
    public StructType getOutputType(GraphSchema graphSchema) {
        List<TableField> fields = new ArrayList<>();
        fields.add(new TableField("cycle_path", StringType.INSTANCE, true));
        return new StructType(fields);
    }

}