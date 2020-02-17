package de.hpi.msc.jschneider.protocol.edgeCreation.worker;

import de.hpi.msc.jschneider.Debug;
import de.hpi.msc.jschneider.data.graph.GraphEdge;
import de.hpi.msc.jschneider.data.graph.GraphNode;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.edgeCreation.EdgeCreationEvents;
import de.hpi.msc.jschneider.protocol.edgeCreation.EdgeCreationMessages;
import de.hpi.msc.jschneider.protocol.nodeCreation.NodeCreationEvents;
import de.hpi.msc.jschneider.protocol.nodeCreation.NodeCreationMessages;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorId;
import de.hpi.msc.jschneider.utility.Counter;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import lombok.val;
import lombok.var;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;

public class EdgeCreationWorkerControl extends AbstractProtocolParticipantControl<EdgeCreationWorkerModel>
{
    public EdgeCreationWorkerControl(EdgeCreationWorkerModel model)
    {
        super(model);
    }

    @Override
    public void preStart()
    {
        super.preStart();

        subscribeToLocalEvent(ProtocolType.NodeCreation, NodeCreationEvents.ResponsibilitiesReceivedEvent.class);
        subscribeToLocalEvent(ProtocolType.NodeCreation, NodeCreationEvents.IntersectionsCalculatedEvent.class);
    }

    @Override
    public ImprovedReceiveBuilder complementReceiveBuilder(ImprovedReceiveBuilder builder)
    {
        return super.complementReceiveBuilder(builder)
                    .match(NodeCreationEvents.ResponsibilitiesReceivedEvent.class, this::onResponsibilitiesReceived)
                    .match(NodeCreationEvents.IntersectionsCalculatedEvent.class, this::onIntersectionsCalculated)
                    .match(EdgeCreationMessages.LastNodeMessage.class, this::onLastNode)
                    .match(NodeCreationMessages.NodesMessage.class, this::onNodes);
    }

    private void onResponsibilitiesReceived(NodeCreationEvents.ResponsibilitiesReceivedEvent message)
    {
        try
        {
            assert getModel().getLocalSegments() == null : "Already received intersection segment responsibilities!";

            getModel().setLocalSegments(message.getSegmentResponsibilities().get(ProcessorId.of(getModel().getSelf())));
            getModel().setLocalSubSequences(message.getSubSequenceResponsibilities().get(ProcessorId.of(getModel().getSelf())));
            getModel().setNumberOfIntersectionSegments(message.getNumberOfIntersectionSegments());

            setNextResponsibleProcessor(message);

            var nextExpectedSubSequenceIndex = getModel().getLocalSubSequences().getFrom();
            if (nextExpectedSubSequenceIndex > 0L)
            {
                // we must have received a reduced sub sequence from out predecessor processor
                nextExpectedSubSequenceIndex -= 1L;
            }

            getModel().setNextSubSequenceIndex(new Counter(nextExpectedSubSequenceIndex));
            enqueueIntersections();
        }
        finally
        {
            complete(message);
        }
    }

    private void setNextResponsibleProcessor(NodeCreationEvents.ResponsibilitiesReceivedEvent message)
    {
        val nextResponsibleProcessorId = message.getSubSequenceResponsibilities()
                                                .entrySet()
                                                .stream()
                                                .filter(entry -> entry.getValue().getFrom() == getModel().getLocalSubSequences().getTo())
                                                .map(Map.Entry::getKey)
                                                .findFirst();

        if (!nextResponsibleProcessorId.isPresent())
        {
            return;
        }

        val protocol = getProtocol(nextResponsibleProcessorId.get(), ProtocolType.EdgeCreation);
        assert protocol.isPresent() : "The next responsible processor must also implement the edge creation protocol!";

        getModel().setNextResponsibleProcessor(protocol.get().getRootActor());
    }

    private void onIntersectionsCalculated(NodeCreationEvents.IntersectionsCalculatedEvent message)
    {
        try
        {
            assert getModel().getIntersectionsInSegment().get(message.getIntersectionCollection().getIntersectionSegment()) == null
                    : "Intersections for this segment have already been calculated!";

            getModel().getIntersectionsInSegment().put(message.getIntersectionCollection().getIntersectionSegment(),
                                                       LocalIntersection.fromIntersectionCollection(message.getIntersectionCollection()));
            enqueueIntersections();
        }
        finally
        {
            complete(message);
        }
    }

    private void enqueueIntersections()
    {
        if (!isReadyToEnqueueIntersections() || getModel().getIntersectionsToMatch() != null)
        {
            return;
        }

        val allIntersections = new LinkedList<LocalIntersection>();
        for (val part : getModel().getIntersectionsInSegment().values())
        {
            allIntersections.addAll(part);
        }

        allIntersections.sort(Comparator.comparingLong(LocalIntersection::getCreationIndex));

        getModel().setIntersectionsToMatch(allIntersections);

        Debug.print(getModel().getIntersectionsToMatch().toArray(new LocalIntersection[0]), String.format("%1$s-intersection-creation-order.txt", ProcessorId.of(getModel().getSelf())));

        getLog().info(String.format("Number of enqueued intersections: %1$d.", allIntersections.size()));
        createEdges();
    }

    private boolean isReadyToEnqueueIntersections()
    {
        if (getModel().getNumberOfIntersectionSegments() == 0)
        {
            return false;
        }

        if (getModel().getIntersectionsInSegment().size() != getModel().getNumberOfIntersectionSegments())
        {
            return false;
        }

        for (var segment = 0; segment < getModel().getNumberOfIntersectionSegments(); ++segment)
        {
            if (!getModel().getIntersectionsInSegment().containsKey(segment))
            {
                return false;
            }
        }

        return true;
    }

    private void onNodes(NodeCreationMessages.NodesMessage message)
    {
        try
        {
            assert getModel().getNodesInSegment().get(message.getIntersectionSegment()) == null
                    : "Nodes for this segment have already been created!";

            getModel().getNodesInSegment().put(message.getIntersectionSegment(), message.getNodes());

            trySendLastNodeToNextResponsibleProcessor(message.getIntersectionSegment());

            createEdges();
        }
        finally
        {
            complete(message);
        }
    }

    private void trySendLastNodeToNextResponsibleProcessor(int intersectionSegment)
    {
        if (getModel().getNextResponsibleProcessor() == null)
        {
            return;
        }

        val lastNode = tryPeekLastNode(intersectionSegment);
        if (!lastNode.isPresent())
        {
            return;
        }

        send(EdgeCreationMessages.LastNodeMessage.builder()
                                                 .sender(getModel().getSelf())
                                                 .receiver(getModel().getNextResponsibleProcessor())
                                                 .lastNode(lastNode.get())
                                                 .build());
    }

    private Optional<GraphNode> tryPeekLastNode(int intersectionSegment)
    {
        if (getModel().getIntersectionsToMatch() == null || getModel().getIntersectionsToMatch().isEmpty())
        {
            return Optional.empty();
        }

        val lastIntersection = getModel().getIntersectionsToMatch().get(getModel().getIntersectionsToMatch().size() - 1);
        if (lastIntersection.getIntersectionSegment() != intersectionSegment)
        {
            return Optional.empty();
        }


        return Optional.of(findClosestNode(lastIntersection));
    }

    private void onLastNode(EdgeCreationMessages.LastNodeMessage message)
    {
        try
        {
            assert getModel().getLastNode() == null : "Already received last node!";

            getModel().setLastNode(message.getLastNode());
            createEdges();
        }
        finally
        {
            complete(message);
        }
    }

    private void createEdges()
    {
        if (!isReadyToCreateEdges())
        {
            return;
        }

        while (!getModel().getIntersectionsToMatch().isEmpty())
        {
            val intersection = getModel().getIntersectionsToMatch().get(0);
            assert intersection != null : "Queued intersections must not be null!";

            if (getModel().getNodesInSegment().get(intersection.getIntersectionSegment()) == null)
            {
                // we did not receive these nodes yet
                return;
            }

            assert intersection.getSubSequenceIndex() >= getModel().getNextSubSequenceIndex().get() : "Unexpected sub sequence index!";

            while (intersection.getSubSequenceIndex() > getModel().getNextSubSequenceIndex().get())
            {
                val subSequenceIndex = getModel().getNextSubSequenceIndex().getAndIncrement();
                val lastNode = getModel().getLastNode();
                if (lastNode == null)
                {
                    continue;
                }

                getModel().getGraph().addEdge(subSequenceIndex, lastNode, lastNode);
            }

            val matchedNode = findClosestNode(intersection);
            getModel().getIntersectionsToMatch().remove(0);

            if (intersection.getSubSequenceIndex() == getModel().getNextSubSequenceIndex().get())
            {
                getModel().getNextSubSequenceIndex().increment();
            }

            if (getModel().getLastNode() != null)
            {
                getModel().getGraph().addEdge(getModel().getNextSubSequenceIndex().get() - 1, getModel().getLastNode(), matchedNode);
            }

            getModel().setLastNode(matchedNode);
        }

        if (!getModel().getIntersectionsToMatch().isEmpty())
        {
            return;
        }

        getModel().getNextSubSequenceIndex().increment();

        while (getModel().getLocalSubSequences().contains(getModel().getNextSubSequenceIndex().get()))
        {
            val subSequenceIndex = getModel().getNextSubSequenceIndex().getAndIncrement();
            getModel().getGraph().addEdge(subSequenceIndex, getModel().getLastNode(), getModel().getLastNode());
        }

        val summedEdgeWeights = getModel().getGraph().getEdges().values().stream().mapToLong(GraphEdge::getWeight).sum();

        getLog().info(String.format("Done creating local graph partition (#nodes = %1$d, #edges = %2$d, tot. edge weights = %3$d) for sub sequences [%4$d, %5$d).",
                                    getModel().getGraph().getNodes().size(),
                                    getModel().getGraph().getEdges().size(),
                                    summedEdgeWeights,
                                    getModel().getLocalSubSequences().getFrom(),
                                    getModel().getLocalSubSequences().getTo()));

        trySendEvent(ProtocolType.EdgeCreation, eventDispatcher -> EdgeCreationEvents.LocalGraphPartitionCreatedEvent.builder()
                                                                                                                     .sender(getModel().getSelf())
                                                                                                                     .receiver(eventDispatcher)
                                                                                                                     .graphPartition(getModel().getGraph())
                                                                                                                     .build());
    }

    private boolean isReadyToCreateEdges()
    {
        return getModel().getIntersectionsToMatch() != null
               && (getModel().getLocalSubSequences().getFrom() == 0L || getModel().getLastNode() != null);
    }

    private GraphNode findClosestNode(LocalIntersection intersection)
    {
        val nodes = getModel().getNodesInSegment().get(intersection.getIntersectionSegment());
        assert nodes != null : "Nodes must not be null to match an intersection! (That should have been checked earlier)";

        var closestIndex = 0;
        var closestDistance = Double.MAX_VALUE;
        for (var nodeIndex = 0; nodeIndex < nodes.length; ++nodeIndex)
        {
            val distance = Math.abs(nodes[nodeIndex] - intersection.getIntersectionDistance());
//            if (distance < closestDistance)
//            {
//                closestIndex = nodeIndex;
//                closestDistance = distance;
//            }
            if (distance >= closestDistance)
            {
                // since the nodes are already sorted by their distance to the origin,
                // we can safely assume that once the distance is getting bigger we
                // have found the best match
                break;
            }

            closestIndex = nodeIndex;
            closestDistance = distance;
        }

        return GraphNode.builder()
                        .intersectionSegment(intersection.getIntersectionSegment())
                        .index(closestIndex)
                        .build();
    }
}
