package de.hpi.msc.jschneider.protocol.scoring.worker;

import akka.testkit.TestProbe;
import com.google.common.primitives.Ints;
import de.hpi.msc.jschneider.data.graph.GraphEdge;
import de.hpi.msc.jschneider.protocol.ProtocolTestCase;
import de.hpi.msc.jschneider.protocol.TestProcessor;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.eventDispatcher.EventDispatcherMessages;
import de.hpi.msc.jschneider.protocol.edgeCreation.EdgeCreationEvents;
import de.hpi.msc.jschneider.protocol.graphMerging.GraphMergingEvents;
import de.hpi.msc.jschneider.protocol.nodeCreation.NodeCreationEvents;
import de.hpi.msc.jschneider.protocol.scoring.ScoringMessages;
import lombok.val;

import static org.assertj.core.api.Assertions.assertThat;

public class TestScoringWorkerControl extends ProtocolTestCase
{
    private TestProcessor remoteProcessor;
    private TestProbe remoteActor;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();

        remoteProcessor = createSlave();
        remoteActor = remoteProcessor.createActor("RemoteActor");
    }

    @Override
    protected ProtocolType[] getProcessorProtocols()
    {
        return new ProtocolType[]{ProtocolType.MessageExchange, ProtocolType.NodeCreation, ProtocolType.EdgeCreation, ProtocolType.GraphMerging, ProtocolType.Scoring};
    }

    private ScoringWorkerModel dummyModel()
    {
        return finalizeModel(ScoringWorkerModel.builder()
                                               .build());
    }

    private ScoringWorkerControl control()
    {
        return new ScoringWorkerControl(dummyModel());
    }

    public void testSubscribeToEvents()
    {
        val control = control();

        control.preStart();

        val responsibilitiesReceivedSubscription = localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(EventDispatcherMessages.SubscribeToEventMessage.class);
        assertThat(responsibilitiesReceivedSubscription.getReceiver().path().root()).isEqualTo(self.ref().path().root());
        assertThat(responsibilitiesReceivedSubscription.getEventType()).isEqualTo(NodeCreationEvents.ResponsibilitiesReceivedEvent.class);

        val localGraphPartitionCreatedSubscription = localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(EventDispatcherMessages.SubscribeToEventMessage.class);
        assertThat(localGraphPartitionCreatedSubscription.getReceiver().path().root()).isEqualTo(self.ref().path().root());
        assertThat(localGraphPartitionCreatedSubscription.getEventType()).isEqualTo(EdgeCreationEvents.LocalGraphPartitionCreatedEvent.class);

        val graphReceivedSubscription = localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(EventDispatcherMessages.SubscribeToEventMessage.class);
        assertThat(graphReceivedSubscription.getReceiver().path().root()).isEqualTo(self.ref().path().root());
        assertThat(graphReceivedSubscription.getEventType()).isEqualTo(GraphMergingEvents.GraphReceivedEvent.class);
    }

    public void testReceiveResponsibilities()
    {
        val control = control();
        val messageInterface = createMessageInterface(control);

        assertThat(control.getModel().isResponsibilitiesReceived()).isFalse();

        val event = createResponsibilitiesReceivedEvent(self, self, 180, 100L, remoteActor, self);
        messageInterface.apply(event);

        assertThat(control.getModel().isResponsibilitiesReceived()).isTrue();
        assertThat(control.getModel().getProcessorResponsibleForPreviousSubSequences()).isEqualTo(remoteProcessor.getProtocolRootActor(ProtocolType.Scoring).ref());

        assertThatMessageIsCompleted(event);
    }

    public void testSendOverlappingEdgeCreationOrder()
    {
        val control = control();
        val messageInterface = createMessageInterface(control);

        val responsibilitiesReceivedEvent = createResponsibilitiesReceivedEvent(self, self, 180, 100L, remoteActor, self);
        messageInterface.apply(responsibilitiesReceivedEvent);
        assertThatMessageIsCompleted(responsibilitiesReceivedEvent);

        val queryLengthMessage = ScoringMessages.QueryPathLengthMessage.builder()
                                                                       .sender(self.ref())
                                                                       .receiver(self.ref())
                                                                       .queryPathLength(50)
                                                                       .build();
        messageInterface.apply(queryLengthMessage);
        assertThat(control.getModel().getQueryPathLength()).isEqualTo(50);
        assertThatMessageIsCompleted(queryLengthMessage);

        val localGraphPartition = createGraph(360, 180);
        val localGraphPartitionCreatedEvent = EdgeCreationEvents.LocalGraphPartitionCreatedEvent.builder()
                                                                                                .sender(self.ref())
                                                                                                .receiver(self.ref())
                                                                                                .edgeCreationOrder(Ints.toArray(localGraphPartition.getEdgeCreationOrder()))
                                                                                                .edges(localGraphPartition.getEdges().values().toArray(new GraphEdge[0]))
                                                                                                .build();
        messageInterface.apply(localGraphPartitionCreatedEvent);
        assertThat(control.getModel().getEdgeCreationOrder()).containsExactly(localGraphPartition.getEdgeCreationOrder().toArray(new Integer[0]));

        val overlappingEdgeCreationOrder = localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(ScoringMessages.OverlappingEdgeCreationOrder.class);
        assertThat(overlappingEdgeCreationOrder.getReceiver()).isEqualTo(remoteProcessor.getProtocolRootActor(ProtocolType.Scoring).ref());
        assertThat(overlappingEdgeCreationOrder.getOverlappingEdgeCreationOrder()).containsExactly(Ints.toArray(localGraphPartition.getEdgeCreationOrder().subList(0, 49)));
        assertThatMessageIsCompleted(localGraphPartitionCreatedEvent);
    }
}
