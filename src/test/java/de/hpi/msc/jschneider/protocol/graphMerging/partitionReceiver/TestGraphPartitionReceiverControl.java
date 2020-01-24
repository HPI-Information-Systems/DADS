package de.hpi.msc.jschneider.protocol.graphMerging.partitionReceiver;

import akka.actor.RootActorPath;
import akka.testkit.TestProbe;
import de.hpi.msc.jschneider.protocol.ProtocolTestCase;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.eventDispatcher.EventDispatcherMessages;
import de.hpi.msc.jschneider.protocol.graphMerging.GraphMergingMessages;
import de.hpi.msc.jschneider.protocol.nodeCreation.NodeCreationEvents;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorId;
import de.hpi.msc.jschneider.utility.Serialize;
import de.hpi.msc.jschneider.utility.dataTransfer.DataTransferMessages;
import lombok.val;
import scala.PartialFunction;
import scala.runtime.BoxedUnit;

import java.util.Arrays;
import java.util.UUID;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class TestGraphPartitionReceiverControl extends ProtocolTestCase
{
    private TestProbe graphMerger;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();

        graphMerger = localProcessor.createActor("GraphPartitionSender");
    }

    @Override
    protected ProtocolType[] getProcessorProtocols()
    {
        return new ProtocolType[]{ProtocolType.MessageExchange, ProtocolType.NodeCreation, ProtocolType.GraphMerging};
    }

    private GraphPartitionReceiverModel dummyModel()
    {
        return finalizeModel(GraphPartitionReceiverModel.builder()
                                                        .graphMerger(graphMerger.ref())
                                                        .build());
    }

    private GraphPartitionReceiverControl control()
    {
        return new GraphPartitionReceiverControl(dummyModel());
    }

    private void sendResponsibilitiesCreatedEvent(GraphPartitionReceiverControl control,
                                                  PartialFunction<Object, BoxedUnit> messageInterface,
                                                  int numberOfIntersectionSegments,
                                                  long numberOfSubSequences,
                                                  TestProbe... participants)
    {
        val event = createResponsibilitiesReceivedEvent(self, self, numberOfIntersectionSegments, numberOfSubSequences, participants);
        messageInterface.apply(event);

        val workerSystems = Arrays.stream(participants).map(participant -> ProcessorId.of(participant.ref())).toArray(ProcessorId[]::new);
        assertThat(control.getModel().getRunningDataTransfers()).containsExactlyInAnyOrder(workerSystems);
        assertThat(control.getModel().getWorkerSystems()).containsExactlyInAnyOrder(workerSystems);

        assertThatMessageIsCompleted(event);
    }

    public void testSubscribeToEvents()
    {
        val control = control();

        control.preStart();

        val responsibilitiesReceivedSubscription = localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(EventDispatcherMessages.SubscribeToEventMessage.class);
        assertThat(ProcessorId.of(responsibilitiesReceivedSubscription.getReceiver())).isEqualTo(localProcessor.getId());
        assertThat(responsibilitiesReceivedSubscription.getEventType()).isEqualTo(NodeCreationEvents.ResponsibilitiesReceivedEvent.class);
    }

    public void testSetRunningDataTransfers()
    {
        val control = control();
        val messageInterface = createMessageInterface(control);

        sendResponsibilitiesCreatedEvent(control, messageInterface, 180, 100L, self);
    }

    public void testSendReceivedEdgesToGraphMerger()
    {
        val control = control();
        val messageInterface = createMessageInterface(control);

        sendResponsibilitiesCreatedEvent(control, messageInterface, 180, 100L, self);

        val operationId = UUID.randomUUID().toString();

        val initializeTransferMessage = GraphMergingMessages.InitializeEdgePartitionTransferMessage.builder()
                                                                                                   .sender(self.ref())
                                                                                                   .receiver(self.ref())
                                                                                                   .operationId(operationId)
                                                                                                   .build();
        messageInterface.apply(initializeTransferMessage);

        localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(DataTransferMessages.RequestNextDataPartMessage.class);
        assertThatMessageIsCompleted(initializeTransferMessage);

        val edges = createGraphEdges("{0_0} -[3]-> {1_1}",
                                     "{1_1} -[2]-> {0_0}");
        val partMessage = DataTransferMessages.DataPartMessage.builder()
                                                              .sender(self.ref())
                                                              .receiver(self.ref())
                                                              .operationId(operationId)
                                                              .part(Serialize.toBytes(edges))
                                                              .isLastPart(false)
                                                              .build();
        messageInterface.apply(partMessage);

        val edgesReceivedMessage = localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(GraphMergingMessages.EdgesReceivedMessage.class);
        assertThat(edgesReceivedMessage.getReceiver()).isEqualTo(graphMerger.ref());
        assertThat(edgesReceivedMessage.getEdges()).containsExactlyInAnyOrder(edges);

        localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(DataTransferMessages.RequestNextDataPartMessage.class);
        assertThatMessageIsCompleted(partMessage);
    }

    public void testSendAllEdgesReceivedToGraphMerger()
    {
        val control = control();
        val messageInterface = createMessageInterface(control);

        sendResponsibilitiesCreatedEvent(control, messageInterface, 180, 100L, self);

        val operationId = UUID.randomUUID().toString();

        val initializeTransferMessage = GraphMergingMessages.InitializeEdgePartitionTransferMessage.builder()
                                                                                                   .sender(self.ref())
                                                                                                   .receiver(self.ref())
                                                                                                   .operationId(operationId)
                                                                                                   .build();
        messageInterface.apply(initializeTransferMessage);

        localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(DataTransferMessages.RequestNextDataPartMessage.class);
        assertThatMessageIsCompleted(initializeTransferMessage);

        val edges = createGraphEdges("{0_0} -[3]-> {1_1}",
                                     "{1_1} -[2]-> {0_0}");
        val partMessage = DataTransferMessages.DataPartMessage.builder()
                                                              .sender(self.ref())
                                                              .receiver(self.ref())
                                                              .operationId(operationId)
                                                              .part(Serialize.toBytes(edges))
                                                              .isLastPart(true)
                                                              .build();
        messageInterface.apply(partMessage);

        val edgesReceivedMessage = localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(GraphMergingMessages.EdgesReceivedMessage.class);
        assertThat(edgesReceivedMessage.getReceiver()).isEqualTo(graphMerger.ref());
        assertThat(edgesReceivedMessage.getEdges()).containsExactlyInAnyOrder(edges);

        val allEdgesReceivedMessage = localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(GraphMergingMessages.AllEdgesReceivedMessage.class);
        assertThat(allEdgesReceivedMessage.getReceiver()).isEqualTo(graphMerger.ref());

        assertThatMessageIsCompleted(partMessage);
    }
}
