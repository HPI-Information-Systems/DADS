package de.hpi.msc.jschneider.protocol.graphMerging.merger;

import akka.actor.RootActorPath;
import de.hpi.msc.jschneider.data.graph.GraphEdge;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.graphMerging.GraphMergingMessages;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import de.hpi.msc.jschneider.utility.dataTransfer.source.GenericDataSource;
import lombok.val;

public class GraphMergerControl extends AbstractProtocolParticipantControl<GraphMergerModel>
{
    public GraphMergerControl(GraphMergerModel model)
    {
        super(model);
    }

    @Override
    public ImprovedReceiveBuilder complementReceiveBuilder(ImprovedReceiveBuilder builder)
    {
        return super.complementReceiveBuilder(builder)
                    .match(GraphMergingMessages.EdgesReceivedMessage.class, this::onEdgesReceived)
                    .match(GraphMergingMessages.AllEdgesReceivedMessage.class, this::onAllEdgesReceived);
    }

    private void onEdgesReceived(GraphMergingMessages.EdgesReceivedMessage message)
    {
        try
        {
            for (val edge : message.getEdges())
            {
                addEdge(edge);
            }
        }
        finally
        {
            complete(message);
        }
    }

    private void addEdge(GraphEdge edge)
    {
        val hash = edge.hashCode();
        val existingEdge = getModel().getEdges().get(hash);

        if (existingEdge == null)
        {
            getModel().getEdges().put(hash, edge);
        }
        else
        {
            existingEdge.setWeight(existingEdge.getWeight() + edge.getWeight());
        }
    }

    private void onAllEdgesReceived(GraphMergingMessages.AllEdgesReceivedMessage message)
    {
        try
        {
            publishGraph(message.getWorkerSystems());
        }
        finally
        {
            complete(message);
        }
    }

    private void publishGraph(RootActorPath[] workerSystems)
    {
        for (val workerSystem : workerSystems)
        {
            val protocol = getProtocol(workerSystem, ProtocolType.GraphMerging);
            assert protocol.isPresent()
                    : String.format("Unable to transfer graph to %1$s, because the processor does not implement the required protocol!", workerSystem);

            getModel().getDataTransferManager().transfer(GenericDataSource.create(getModel().getEdges().values().toArray(new GraphEdge[0])),
                                                         dataDistributor -> GraphMergingMessages.InitializeGraphTransferMessage.builder()
                                                                                                                               .sender(getModel().getSelf())
                                                                                                                               .receiver(protocol.get().getRootActor())
                                                                                                                               .operationId(dataDistributor.getOperationId())
                                                                                                                               .build());
        }
    }
}
