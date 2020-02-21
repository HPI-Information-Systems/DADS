package de.hpi.msc.jschneider.protocol.graphMerging.partitionReceiver;

import de.hpi.msc.jschneider.data.graph.GraphEdge;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.graphMerging.GraphMergingMessages;
import de.hpi.msc.jschneider.protocol.nodeCreation.NodeCreationEvents;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorId;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import de.hpi.msc.jschneider.utility.Serialize;
import de.hpi.msc.jschneider.utility.dataTransfer.DataReceiver;
import de.hpi.msc.jschneider.utility.dataTransfer.DataTransferMessages;
import lombok.val;

import java.util.Arrays;

public class GraphPartitionReceiverControl extends AbstractProtocolParticipantControl<GraphPartitionReceiverModel>
{
    public GraphPartitionReceiverControl(GraphPartitionReceiverModel model)
    {
        super(model);
    }

    @Override
    public void preStart()
    {
        super.preStart();

        subscribeToLocalEvent(ProtocolType.NodeCreation, NodeCreationEvents.ResponsibilitiesReceivedEvent.class);
    }

    @Override
    public ImprovedReceiveBuilder complementReceiveBuilder(ImprovedReceiveBuilder builder)
    {
        return super.complementReceiveBuilder(builder)
                    .match(NodeCreationEvents.ResponsibilitiesReceivedEvent.class, this::onResponsibilitiesReceived)
                    .match(GraphMergingMessages.InitializeEdgePartitionTransferMessage.class, this::onInitializeEdgePartitionTransfer);
    }

    private void onResponsibilitiesReceived(NodeCreationEvents.ResponsibilitiesReceivedEvent message)
    {
        try
        {
            assert getModel().getRunningDataTransfers() == null : "Responsibilities were received already!";

            getModel().setRunningDataTransfers(message.getSubSequenceResponsibilities().keySet());
            getModel().setWorkerSystems(getModel().getRunningDataTransfers().toArray(new ProcessorId[0]));
        }
        finally
        {
            complete(message);
        }
    }

    private void onInitializeEdgePartitionTransfer(GraphMergingMessages.InitializeEdgePartitionTransferMessage message)
    {
        assert getModel().getRunningDataTransfers() != null : "Unable to initialize edge partition transfer, because the participating worker systems are unknown!";

        getModel().getDataTransferManager().accept(message, dataReceiver ->
        {
            dataReceiver.setState(ProcessorId.of(message.getSender()));

            return dataReceiver.whenDataPartReceived(this::onEdgePartitionPartReceived)
                               .whenFinished(this::onEdgePartitionTransferFinished);
        });
    }

    private void onEdgePartitionPartReceived(DataTransferMessages.DataPartMessage message)
    {
        if (message.getPart().length < 1)
        {
            return;
        }

        val edges = Serialize.toGraphEdges(message.getPart());
        val summedEdgeWeights = Arrays.stream(edges).mapToLong(GraphEdge::getWeight).sum();

        getLog().info(String.format("Received graph edges (#edges = %1$d, tot. edge weight = %2$d) from %3$s",
                                    edges.length,
                                    summedEdgeWeights,
                                    message.getSender().path().root()));

        send(GraphMergingMessages.EdgesReceivedMessage.builder()
                                                      .sender(getModel().getSelf())
                                                      .receiver(getModel().getGraphMerger())
                                                      .edges(edges)
                                                      .build());
    }

    private void onEdgePartitionTransferFinished(DataReceiver dataReceiver)
    {
        assert dataReceiver.getState() instanceof ProcessorId : "DataReceiver state should be a ProcessorId!";

        val workerSystem = (ProcessorId) dataReceiver.getState();

        getModel().getRunningDataTransfers().remove(workerSystem);

        if (!getModel().getRunningDataTransfers().isEmpty())
        {
            return;
        }

        send(GraphMergingMessages.AllEdgesReceivedMessage.builder()
                                                         .sender(getModel().getSelf())
                                                         .receiver(getModel().getGraphMerger())
                                                         .workerSystems(getModel().getWorkerSystems())
                                                         .build());
    }
}
