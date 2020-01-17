package de.hpi.msc.jschneider.protocol.edgeCreation.worker;

import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.nodeCreation.NodeCreationEvents;
import de.hpi.msc.jschneider.protocol.nodeCreation.NodeCreationMessages;
import de.hpi.msc.jschneider.utility.Counter;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import lombok.val;
import lombok.var;

import java.util.LinkedList;

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
                    .match(NodeCreationEvents.ResponsibilitiesReceivedEvent.class, this::onSegmentResponsibilitiesReceived)
                    .match(NodeCreationEvents.IntersectionsCalculatedEvent.class, this::onIntersectionsCalculated)
                    .match(NodeCreationMessages.NodesMessage.class, this::onNodes);
    }

    private void onSegmentResponsibilitiesReceived(NodeCreationEvents.ResponsibilitiesReceivedEvent message)
    {
        try
        {
            assert getModel().getLocalSegments() == null : "Already received intersection segment responsibilities!";

            getModel().setLocalSegments(message.getSegmentResponsibilities());
            getModel().setLocalSubSequences(message.getSubSequenceResponsibilities());
            getModel().setNextSubSequenceIndex(new Counter(message.getSubSequenceResponsibilities().getFrom()));
            enqueueIntersections();
        }
        finally
        {
            complete(message);
        }
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
        if (!isReadyToEnqueueIntersections())
        {
            return;
        }

        val allIntersections = new LinkedList<LocalIntersection>();
        for (val part : getModel().getIntersectionsInSegment().values())
        {
            allIntersections.addAll(part);
        }

        allIntersections.sort((a, b) ->
                              {
                                  val diff = a.getSubSequenceIndex() - b.getSubSequenceIndex();
                                  if (diff != 0)
                                  {
                                      return (int) diff;
                                  }

                                  return a.getIntersectionSegment() - b.getIntersectionSegment();
                              });

        getModel().setIntersectionsToMatch(allIntersections);

        createEdges();
    }

    private boolean isReadyToEnqueueIntersections()
    {
        if (getModel().getLocalSegments() == null)
        {
            return false;
        }

        if (getModel().getIntersectionsInSegment().size() != getModel().getLocalSegments().length())
        {
            return false;
        }

        for (var segment = getModel().getLocalSegments().getFrom(); segment < getModel().getLocalSegments().getTo(); ++segment)
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
            val intersection = getModel().getIntersectionsToMatch().peek();
            assert intersection != null : "Queued intersections must not be null!";

            if (getModel().getNodesInSegment().get(intersection.getIntersectionSegment()) == null)
            {
                // we did not receive these nodes yet
                break;
            }

            while (intersection.getSubSequenceIndex() > getModel().getNextSubSequenceIndex().get())
            {
                getModel().getNextSubSequenceIndex().increment();
                val lastNode = getModel().getLastNode();
                if (lastNode == null)
                {
                    continue;
                }

                addEdge(lastNode, lastNode);
            }

            val matchedNode = findClosestNode(intersection);
            getModel().getNodes().add(matchedNode);
            getModel().getIntersectionsToMatch().poll();

            if (intersection.getSubSequenceIndex() == getModel().getNextSubSequenceIndex().get())
            {
                getModel().getNextSubSequenceIndex().increment();
            }

            if (getModel().getLastNode() != null)
            {
                addEdge(getModel().getLastNode(), matchedNode);
            }

            getModel().setLastNode(matchedNode);
        }

        if (!getModel().getIntersectionsToMatch().isEmpty())
        {
            return;
        }

        getLog().info(String.format("Done creating local graph partition (#nodes = %1$d, #edges = %2$d) for sub sequences [%3$d, %4$d).",
                                    getModel().getNodes().size(),
                                    getModel().getEdges().size(),
                                    getModel().getLocalSubSequences().getFrom(),
                                    getModel().getLocalSubSequences().getTo()));
    }

    private void addEdge(LocalNode from, LocalNode to)
    {
        val edge = LocalEdge.builder()
                            .from(from)
                            .to(to)
                            .build();
        val hash = edge.hashCode();
        val existingEdge = getModel().getEdges().get(hash);

        if (existingEdge == null)
        {
            getModel().getEdges().put(hash, edge);
        }
        else
        {
            existingEdge.getWeight().increment();
        }
    }

    private boolean isReadyToCreateEdges()
    {
        return getModel().getIntersectionsToMatch() != null;
    }

    private LocalNode findClosestNode(LocalIntersection intersection)
    {
        val nodes = getModel().getNodesInSegment().get(intersection.getIntersectionSegment());
        assert nodes != null : "Nodes must not be null to match an intersection! (That should have been checked earlier)";

        var closestIndex = 0;
        var closestDistance = Float.MAX_VALUE;
        for (var nodeIndex = 0; nodeIndex < nodes.length; ++nodeIndex)
        {
            val distance = Math.abs(nodes[nodeIndex] - intersection.getIntersectionDistance());
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

        return LocalNode.builder()
                        .intersectionSegment(intersection.getIntersectionSegment())
                        .index(closestIndex)
                        .build();
    }
}
