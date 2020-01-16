package de.hpi.msc.jschneider.protocol.edgeCreation.worker;

import de.hpi.msc.jschneider.math.Node;
import de.hpi.msc.jschneider.math.NodeCollection;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.nodeCreation.NodeCreationEvents;
import de.hpi.msc.jschneider.protocol.nodeCreation.NodeCreationMessages;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import lombok.val;

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

        subscribeToLocalEvent(ProtocolType.NodeCreation, NodeCreationEvents.IntersectionsCalculatedEvent.class);
    }

    @Override
    public ImprovedReceiveBuilder complementReceiveBuilder(ImprovedReceiveBuilder builder)
    {
        return super.complementReceiveBuilder(builder)
                    .match(NodeCreationEvents.IntersectionsCalculatedEvent.class, this::onIntersectionsCalculated)
                    .match(NodeCreationMessages.NodesMessage.class, this::onNodes);
    }

    private void onIntersectionsCalculated(NodeCreationEvents.IntersectionsCalculatedEvent message)
    {
        try
        {
            assert getModel().getIntersectionsInSegment().get(message.getIntersectionCollection().getIntersectionSegment()) == null
                    : "Intersections for this segment have already been calculated!";

            getModel().getIntersectionsInSegment().put(message.getIntersectionCollection().getIntersectionSegment(), message.getIntersectionCollection());
            createEdges();
        }
        finally
        {
            complete(message);
        }
    }

    private void onNodes(NodeCreationMessages.NodesMessage message)
    {
        try
        {
            assert getModel().getNodesInSegment().get(message.getIntersectionSegment()) == null
                    : "Nodes for this segment have already been created!";

            val collection = NodeCollection.builder()
                                           .intersectionSegment(message.getIntersectionSegment())
                                           .build();
            for (val nodeIntersectionLength : message.getNodes())
            {
                collection.getNodes().add(Node.builder()
                                              .intersectionLength(nodeIntersectionLength)
                                              .build());
            }

            getModel().getNodesInSegment().put(message.getIntersectionSegment(), collection);
            createEdges();
        }
        finally
        {
            complete(message);
        }
    }

    private void createEdges()
    {
        for (val intersectionSegment : getModel().getIntersectionsInSegment().keySet())
        {

        }
    }

    private void createEdges(int intersectionSegment)
    {

    }
}
