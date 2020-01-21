package de.hpi.msc.jschneider.protocol.graphMerging.partitionReceiver;

import akka.actor.RootActorPath;
import akka.testkit.TestProbe;
import de.hpi.msc.jschneider.protocol.ProtocolTestCase;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.eventDispatcher.EventDispatcherMessages;
import de.hpi.msc.jschneider.protocol.graphMerging.GraphMergingMessages;
import de.hpi.msc.jschneider.protocol.nodeCreation.NodeCreationEvents;
import de.hpi.msc.jschneider.utility.Int32Range;
import de.hpi.msc.jschneider.utility.Int64Range;
import de.hpi.msc.jschneider.utility.Serialize;
import de.hpi.msc.jschneider.utility.dataTransfer.DataTransferMessages;
import lombok.val;
import lombok.var;
import scala.PartialFunction;
import scala.runtime.BoxedUnit;

import java.util.Arrays;
import java.util.HashMap;
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

    public void testSubscribeToEvents()
    {
        val control = control();

        control.preStart();

        val responsibilitiesReceivedSubscription = localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(EventDispatcherMessages.SubscribeToEventMessage.class);
        assertThat(responsibilitiesReceivedSubscription.getReceiver().path().root()).isEqualTo(localProcessor.getRootPath());
        assertThat(responsibilitiesReceivedSubscription.getEventType()).isEqualTo(NodeCreationEvents.ResponsibilitiesReceivedEvent.class);
    }

    private void sendResponsibilities(GraphPartitionReceiverControl control,
                                      PartialFunction<Object, BoxedUnit> messageInterface,
                                      int numberOfIntersectionSegments,
                                      long numberOfSubSequences,
                                      TestProbe... participants)
    {
        val subSequenceResponsibilities = new HashMap<RootActorPath, Int64Range>();
        val subSequencesPerParticipant = Math.ceil(numberOfSubSequences / (double) participants.length);
        val segmentResponsibilities = new HashMap<RootActorPath, Int32Range>();
        val segmentsPerParticipant = Math.ceil(numberOfIntersectionSegments / (double) participants.length);

        for (var participantIndex = 0; participantIndex < participants.length; ++participantIndex)
        {
            val participant = participants[participantIndex];
            subSequenceResponsibilities.put(participant.ref().path().root(),
                                            Int64Range.builder()
                                                      .from((long) (participantIndex * subSequencesPerParticipant))
                                                      .to((long) Math.min(numberOfSubSequences, ((participantIndex + 1) * subSequencesPerParticipant)))
                                                      .build());
            segmentResponsibilities.put(participant.ref().path().root(),
                                        Int32Range.builder()
                                                  .from((int) (participantIndex * segmentsPerParticipant))
                                                  .to((int) Math.min(numberOfIntersectionSegments, (participantIndex + 1) * segmentsPerParticipant))
                                                  .build());
        }

        val message = NodeCreationEvents.ResponsibilitiesReceivedEvent.builder()
                                                                      .sender(self.ref())
                                                                      .receiver(self.ref())
                                                                      .numberOfIntersectionSegments(numberOfIntersectionSegments)
                                                                      .subSequenceResponsibilities(subSequenceResponsibilities)
                                                                      .segmentResponsibilities(segmentResponsibilities)
                                                                      .build();
        messageInterface.apply(message);

        val workerSystems = Arrays.stream(participants).map(participant -> participant.ref().path().root()).toArray(RootActorPath[]::new);
        assertThat(control.getModel().getWorkerSystems()).containsExactlyInAnyOrder(workerSystems);
        assertThat(control.getModel().getRunningDataTransfers()).containsExactlyInAnyOrder(workerSystems);

        assertThatMessageIsCompleted(message);
    }

    public void testSetRunningDataTransfers()
    {
        val control = control();
        val messageInterface = createMessageInterface(control);

        sendResponsibilities(control, messageInterface, 180, 100L, self);
    }

    public void testSendReceivedEdgesToGraphMerger()
    {
        val control = control();
        val messageInterface = createMessageInterface(control);

        sendResponsibilities(control, messageInterface, 180, 100L, self);

        val operationId = UUID.randomUUID();

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

        sendResponsibilities(control, messageInterface, 180, 100L, self);

        val operationId = UUID.randomUUID();

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
