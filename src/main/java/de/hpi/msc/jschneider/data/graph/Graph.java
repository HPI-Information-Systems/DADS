package de.hpi.msc.jschneider.data.graph;

import de.hpi.msc.jschneider.utility.Counter;
import lombok.Getter;
import lombok.NonNull;
import lombok.val;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Graph
{
    @Getter @NonNull
    private final Map<Integer, GraphEdge> edges = new HashMap<>();
    @Getter @NonNull
    private final Map<Long, List<Integer>> createdEdgesBySubSequenceIndex = new HashMap<>();

    public GraphEdge addEdge(long subSequenceIndex, GraphNode from, GraphNode to)
    {
        return addEdge(subSequenceIndex,
                       GraphEdge.builder()
                                .from(from)
                                .to(to)
                                .weight(new Counter(1L))
                                .build());
    }

    public GraphEdge addEdge(long subSequenceIndex, GraphEdge edge)
    {
        assert edge.getWeight() == 1L : "New edges must always have a weight of 1!";

        val hash = edge.hashCode();
        createdEdgesBySubSequenceIndex.putIfAbsent(subSequenceIndex, new ArrayList<>());
        createdEdgesBySubSequenceIndex.get(subSequenceIndex).add(hash);

        val existingEdge = edges.get(hash);
        if (existingEdge != null)
        {
            assert existingEdge.getKey().equals(edge.getKey());

            existingEdge.incrementWeight();
            return existingEdge;
        }

        edges.put(hash, edge);
        return edge;
    }

    public Collection<GraphNode> getNodes()
    {
        return edges.values().stream().flatMap(edge -> Arrays.stream(new GraphNode[]{edge.getFrom(), edge.getTo()})).collect(Collectors.toSet());
    }

    public void add(Graph partition)
    {
        for (val subSequenceIndex : partition.getCreatedEdgesBySubSequenceIndex().keySet())
        {
            for (val edgeHash : partition.getCreatedEdgesBySubSequenceIndex().get(subSequenceIndex))
            {
                val edge = partition.getEdges().get(edgeHash);

                createdEdgesBySubSequenceIndex.putIfAbsent(subSequenceIndex, new ArrayList<>());
                createdEdgesBySubSequenceIndex.get(subSequenceIndex).add(edgeHash);
                val existingEdge = edges.get(edgeHash);
                if (existingEdge != null)
                {
                    existingEdge.incrementWeight();
                }
                else
                {
                    edges.put(edgeHash, GraphEdge.builder()
                                                 .weight(new Counter(1L))
                                                 .from(edge.getFrom())
                                                 .to(edge.getTo())
                                                 .build());
                }
            }
        }
    }
}
