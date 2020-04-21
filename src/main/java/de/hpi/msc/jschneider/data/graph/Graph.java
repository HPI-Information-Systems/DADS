package de.hpi.msc.jschneider.data.graph;

import de.hpi.msc.jschneider.utility.Counter;
import it.unimi.dsi.fastutil.ints.Int2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.IntBigArrayBigList;
import it.unimi.dsi.fastutil.ints.IntBigList;
import it.unimi.dsi.fastutil.longs.Long2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import lombok.Getter;
import lombok.NonNull;
import lombok.val;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

public class Graph
{
    @NonNull @Getter
    private final Int2ObjectMap<GraphEdge> edges = new Int2ObjectLinkedOpenHashMap<>();
    @NonNull @Getter
    private final Long2ObjectMap<IntBigList> createdEdgesBySubSequenceIndex = new Long2ObjectLinkedOpenHashMap<>();

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
        createdEdgesBySubSequenceIndex.putIfAbsent(subSequenceIndex, new IntBigArrayBigList());
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
        val subSequenceIterator = partition.getCreatedEdgesBySubSequenceIndex().keySet().iterator();
        while (subSequenceIterator.hasNext())
        {
            val subSequenceIndex = subSequenceIterator.nextLong();
            val edgeHashIterator = partition.getCreatedEdgesBySubSequenceIndex().get(subSequenceIndex).iterator();
            while (edgeHashIterator.hasNext())
            {
                val edgeHash = edgeHashIterator.nextInt();
                val edge = partition.getEdges().get(edgeHash);

                createdEdgesBySubSequenceIndex.putIfAbsent(subSequenceIndex, new IntBigArrayBigList(partition.getCreatedEdgesBySubSequenceIndex().get(subSequenceIndex).size64()));
                createdEdgesBySubSequenceIndex.get(subSequenceIndex).add(edgeHash);

                val existingEdge = edges.get(edgeHash);
                if (existingEdge != null)
                {
                    existingEdge.incrementWeight();
                }
                else
                {
                    edges.put(edgeHash, GraphEdge.builder().weight(new Counter(1L))
                                                 .from(edge.getFrom())
                                                 .to(edge.getTo())
                                                 .build());
                }
            }
        }
    }
}
