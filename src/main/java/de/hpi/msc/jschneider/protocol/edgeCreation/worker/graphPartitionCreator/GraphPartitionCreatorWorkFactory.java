package de.hpi.msc.jschneider.protocol.edgeCreation.worker.graphPartitionCreator;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.data.graph.GraphNode;
import de.hpi.msc.jschneider.math.Calculate;
import de.hpi.msc.jschneider.protocol.actorPool.ActorPoolMessages;
import de.hpi.msc.jschneider.protocol.actorPool.worker.WorkFactory;
import de.hpi.msc.jschneider.protocol.edgeCreation.worker.LocalIntersection;
import de.hpi.msc.jschneider.utility.Counter;
import de.hpi.msc.jschneider.utility.Int64Range;
import lombok.Getter;
import lombok.val;
import lombok.var;

import java.util.List;
import java.util.Map;

public class GraphPartitionCreatorWorkFactory implements WorkFactory
{
    private static final int CHUNK_SIZE = 5000;

    private final ActorRef supervisor;
    private final List<LocalIntersection> intersections;
    private final Map<Integer, double[]> nodes;
    private final Counter nextSubSequenceIndex;
    private int nextChunkStartIndex = 0;
    private GraphNode lastNode;
    private final Int64Range localSubSequences;
    @Getter
    private final int numberOfChunks;
    private int processedChunks = 0;

    public GraphPartitionCreatorWorkFactory(ActorRef supervisor,
                                            List<LocalIntersection> intersections,
                                            Map<Integer, double[]> nodes,
                                            long firstSubSequenceIndex,
                                            GraphNode lastNode,
                                            Int64Range localSubSequences)
    {
        this.supervisor = supervisor;
        this.intersections = intersections;
        this.nodes = nodes;
        nextSubSequenceIndex = new Counter(firstSubSequenceIndex);
        this.lastNode = lastNode;
        this.localSubSequences = localSubSequences;
        numberOfChunks = (int) Math.max(1, Math.floor(intersections.size() / (double) CHUNK_SIZE));
    }

    @Override
    public boolean hasNext()
    {
        return processedChunks < numberOfChunks;
    }

    @Override
    public ActorPoolMessages.WorkMessage next(ActorRef worker)
    {
        var chunkEnd = Math.min(intersections.size(), nextChunkStartIndex + CHUNK_SIZE);
        val isLastChunk = isLastChunk();
        if (isLastChunk)
        {
            chunkEnd = intersections.size();
        }

        val chunk = intersections.subList(nextChunkStartIndex, chunkEnd);
        val message = GraphPartitionCreatorMessages.CreateGraphPartitionMessage.builder()
                                                                               .sender(supervisor)
                                                                               .receiver(worker)
                                                                               .consumer(new GraphPartitionCreator())
                                                                               .intersections(chunk)
                                                                               .nodes(nodes)
                                                                               .firstSubSequenceIndex(nextSubSequenceIndex.get())
                                                                               .lastNode(lastNode)
                                                                               .localSubSequences(localSubSequences)
                                                                               .lastChunk(isLastChunk)
                                                                               .build();

        val lastIntersection = chunk.get(chunk.size() - 1);
        val nodeValues = nodes.get(lastIntersection.getIntersectionSegment());
        val lastNodeIndex = Calculate.minimumDistanceIndex(lastIntersection.getIntersectionDistance(), nodeValues);
        nextChunkStartIndex += chunk.size();
        nextSubSequenceIndex.set(lastIntersection.getSubSequenceIndex() + 1);
        lastNode = GraphNode.builder()
                            .intersectionSegment(lastIntersection.getIntersectionSegment())
                            .index(lastNodeIndex)
                            .build();
        processedChunks++;

        return message;
    }

    private boolean isLastChunk()
    {
        return processedChunks == numberOfChunks - 1;
    }
}
