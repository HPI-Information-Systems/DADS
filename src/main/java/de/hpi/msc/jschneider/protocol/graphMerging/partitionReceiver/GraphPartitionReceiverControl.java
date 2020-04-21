package de.hpi.msc.jschneider.protocol.graphMerging.partitionReceiver;

import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.graphMerging.GraphMergingMessages;
import de.hpi.msc.jschneider.protocol.nodeCreation.NodeCreationEvents;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorId;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import de.hpi.msc.jschneider.utility.dataTransfer.sink.AdHocGraphEdgeSink;
import lombok.val;

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
            val workerSystem = ProcessorId.of(message.getSender());
            val sink = new AdHocGraphEdgeSink().whenDataPart(s -> onEdgePartReceived(workerSystem, s));
            return dataReceiver.addSink(sink)
                    .whenFinished(receiver -> onEdgePartitionTransferFinished(workerSystem));
        });
    }

    private void onEdgePartReceived(ProcessorId workerSystem, AdHocGraphEdgeSink sink)
    {
        getLog().info("Received graph {} graph edges from {}.", sink.edgesLength(), workerSystem);
        send(GraphMergingMessages.EdgesReceivedMessage.builder()
                                                      .sender(getModel().getSelf())
                                                      .receiver(getModel().getGraphMerger())
                                                      .edges(sink.edges())
                                                      .edgesLength(sink.edgesLength())
                                                      .build());
    }

    private void onEdgePartitionTransferFinished(ProcessorId workerSystem)
    {
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

        isReadyToBeTerminated();
    }
}
