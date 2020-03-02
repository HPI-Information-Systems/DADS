package de.hpi.msc.jschneider.protocol.edgeCreation.worker.graphPartitionCreator;

import de.hpi.msc.jschneider.data.graph.Graph;
import de.hpi.msc.jschneider.data.graph.GraphNode;
import de.hpi.msc.jschneider.math.Calculate;
import de.hpi.msc.jschneider.protocol.actorPool.ActorPoolMessages;
import de.hpi.msc.jschneider.protocol.actorPool.worker.ActorPoolWorkerControl;
import de.hpi.msc.jschneider.protocol.actorPool.worker.WorkConsumer;
import de.hpi.msc.jschneider.utility.Counter;
import lombok.val;
import lombok.var;

public class GraphPartitionCreator implements WorkConsumer
{
    @Override
    public void process(ActorPoolWorkerControl control, ActorPoolMessages.WorkMessage workLoad)
    {
        assert workLoad instanceof GraphPartitionCreatorMessages.CreateGraphPartitionMessage : "Unexpected work!";
        process(control, (GraphPartitionCreatorMessages.CreateGraphPartitionMessage) workLoad);
    }

    private void process(ActorPoolWorkerControl control, GraphPartitionCreatorMessages.CreateGraphPartitionMessage message)
    {
        val graph = new Graph();
        val nextSubSequenceIndex = new Counter(message.getFirstSubSequenceIndex());
        var lastNode = message.getLastNode();
        for (var intersectionsIndex = 0; intersectionsIndex < message.getIntersections().size(); ++intersectionsIndex)
        {
            val intersection = message.getIntersections().get(intersectionsIndex);
            assert intersection.getSubSequenceIndex() >= nextSubSequenceIndex.get() : "Unexpected sub sequence index!";

            while (intersection.getSubSequenceIndex() > nextSubSequenceIndex.get())
            {
                val subSequenceIndex = nextSubSequenceIndex.getAndIncrement();
                if (lastNode == null)
                {
                    continue;
                }

                graph.addEdge(subSequenceIndex, lastNode, lastNode);
            }

            val nodeIndex = Calculate.minimumDistanceIndexSorted(intersection.getIntersectionDistance(), message.getNodes().get(intersection.getIntersectionSegment()));
            val matchedNode = GraphNode.builder()
                                       .intersectionSegment(intersection.getIntersectionSegment())
                                       .index(nodeIndex)
                                       .build();

            if (intersection.getSubSequenceIndex() == nextSubSequenceIndex.get())
            {
                nextSubSequenceIndex.increment();
            }

            if (lastNode != null)
            {
                graph.addEdge(nextSubSequenceIndex.get() - 1, lastNode, matchedNode);
            }


            lastNode = matchedNode;
        }

        nextSubSequenceIndex.increment();

        while (message.isLastChunk() && message.getLocalSubSequences().contains(nextSubSequenceIndex.get()))
        {
            val subSequenceIndex = nextSubSequenceIndex.getAndIncrement();
            graph.addEdge(subSequenceIndex, lastNode, lastNode);
        }

        control.send(GraphPartitionCreatorMessages.GraphPartitionCreatedMessage.builder()
                                                                               .sender(control.getModel().getSelf())
                                                                               .receiver(message.getSender())
                                                                               .firstIntersectionCreationIndex(message.getIntersections().get(0).getCreationIndex())
                                                                               .graphPartition(graph)
                                                                               .build());
    }
}
