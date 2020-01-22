package de.hpi.msc.jschneider.data.graph;

import lombok.Getter;
import lombok.NonNull;
import lombok.val;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Graph
{
    @Getter @NonNull
    private final Map<Integer, GraphEdge> edges = new HashMap<>();
    @Getter @NonNull
    private final List<Integer> edgeCreationOrder = new ArrayList<>();

    public static Graph construct(GraphEdge[] edges, int[] edgeCreationOrder)
    {
        val graph = new Graph();
        for (val edge : edges)
        {
            val hash = edge.hashCode();
            graph.edges.put(hash, edge);
        }
        graph.edgeCreationOrder.addAll(Arrays.stream(edgeCreationOrder).boxed().collect(Collectors.toList()));

        return graph;
    }

    public void addEdge(GraphEdge edge)
    {
        assert edge.getWeight() == 1L : "New edges must always have a weight of 1!";

        val hash = edge.hashCode();
        val existingEdge = edges.get(hash);
        if (existingEdge != null)
        {
            existingEdge.incrementWeight();
        }
        else
        {
            edges.put(hash, edge);
        }

        edgeCreationOrder.add(hash);
    }
}
