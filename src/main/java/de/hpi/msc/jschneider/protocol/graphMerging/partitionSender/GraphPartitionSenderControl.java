package de.hpi.msc.jschneider.protocol.graphMerging.partitionSender;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.data.graph.GraphEdge;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.edgeCreation.EdgeCreationEvents;
import de.hpi.msc.jschneider.protocol.graphMerging.GraphMergingMessages;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import de.hpi.msc.jschneider.utility.dataTransfer.source.GenericDataSource;
import lombok.val;

public class GraphPartitionSenderControl extends AbstractProtocolParticipantControl<GraphPartitionSenderModel>
{
    public GraphPartitionSenderControl(GraphPartitionSenderModel model)
    {
        super(model);
    }

    @Override
    public void preStart()
    {
        super.preStart();

        subscribeToLocalEvent(ProtocolType.EdgeCreation, EdgeCreationEvents.LocalGraphPartitionCreatedEvent.class);
    }

    @Override
    public ImprovedReceiveBuilder complementReceiveBuilder(ImprovedReceiveBuilder builder)
    {
        return super.complementReceiveBuilder(builder)
                    .match(EdgeCreationEvents.LocalGraphPartitionCreatedEvent.class, this::onLocalGraphPartitionCreated);
    }

    private void onLocalGraphPartitionCreated(EdgeCreationEvents.LocalGraphPartitionCreatedEvent message)
    {
        try
        {
            val receiverProtocol = getMasterProtocol(ProtocolType.GraphMerging);
            assert receiverProtocol.isPresent() : "Unable to send local graph partition to master!";

            initializeEdgePartitionTransfer(message, receiverProtocol.get().getRootActor());
        }
        finally
        {
            complete(message);
        }
    }

    private void initializeEdgePartitionTransfer(EdgeCreationEvents.LocalGraphPartitionCreatedEvent message, ActorRef receiver)
    {
        getLog().info(String.format("Start transferring edge partition to %1$s.", receiver.path().root()));

        val edges = message.getGraphPartition().getEdges().values().toArray(new GraphEdge[0]);

        getModel().getDataTransferManager().transfer(GenericDataSource.create(edges),
                                                     (dataDistributor, operationId) -> GraphMergingMessages.InitializeEdgePartitionTransferMessage.builder()
                                                                                                                                                  .sender(dataDistributor)
                                                                                                                                                  .receiver(receiver)
                                                                                                                                                  .operationId(operationId)
                                                                                                                                                  .build());
    }
}
