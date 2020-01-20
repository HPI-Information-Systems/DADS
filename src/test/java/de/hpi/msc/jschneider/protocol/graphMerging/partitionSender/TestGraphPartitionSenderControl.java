package de.hpi.msc.jschneider.protocol.graphMerging.partitionSender;

import de.hpi.msc.jschneider.protocol.ProtocolTestCase;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.eventDispatcher.EventDispatcherMessages;
import de.hpi.msc.jschneider.protocol.edgeCreation.EdgeCreationEvents;
import de.hpi.msc.jschneider.protocol.graphMerging.GraphMergingMessages;
import lombok.val;

import static org.assertj.core.api.Assertions.assertThat;

public class TestGraphPartitionSenderControl extends ProtocolTestCase
{
    @Override
    protected ProtocolType[] getProcessorProtocols()
    {
        return new ProtocolType[]{ProtocolType.MessageExchange, ProtocolType.EdgeCreation, ProtocolType.GraphMerging};
    }

    private GraphPartitionSenderModel dummyModel()
    {
        return finalizeModel(GraphPartitionSenderModel.builder()
                                                      .build());
    }

    private GraphPartitionSenderControl control()
    {
        return new GraphPartitionSenderControl(dummyModel());
    }

    public void testSubscribeToEvents()
    {
        val control = control();

        control.preStart();

        val localGraphPartitionCreatedSubscription = localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(EventDispatcherMessages.SubscribeToEventMessage.class);
        assertThat(localGraphPartitionCreatedSubscription.getEventType()).isEqualTo(EdgeCreationEvents.LocalGraphPartitionCreatedEvent.class);
        assertThat(localGraphPartitionCreatedSubscription.getReceiver().path().root()).isEqualTo(localProcessor.getRootPath());
    }

    public void testInitializeEdgePartitionTransfer()
    {
        val control = control();
        val messageInterface = createMessageInterface(control);

        val edges = createGraphEdges("{0_0} -[3]-> {1_1}",
                                     "{1_1} -[2]-> {0_0}");
        val edgeCreationOrder = new int[]{0, 1, 2, 3, 4}; // this contains the edge hashes

        val message = EdgeCreationEvents.LocalGraphPartitionCreatedEvent.builder()
                                                                        .sender(self.ref())
                                                                        .receiver(self.ref())
                                                                        .edges(edges)
                                                                        .edgeCreationOrder(edgeCreationOrder)
                                                                        .build();
        messageInterface.apply(message);

        localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(GraphMergingMessages.InitializeEdgePartitionTransferMessage.class);
        assertThatMessageIsCompleted(message);
    }
}
