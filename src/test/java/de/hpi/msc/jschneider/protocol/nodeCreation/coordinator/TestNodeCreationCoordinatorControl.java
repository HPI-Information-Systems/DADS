package de.hpi.msc.jschneider.protocol.nodeCreation.coordinator;

import akka.testkit.TestProbe;
import de.hpi.msc.jschneider.protocol.ProtocolTestCase;
import de.hpi.msc.jschneider.protocol.TestProcessor;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.nodeCreation.NodeCreationEvents;
import de.hpi.msc.jschneider.protocol.nodeCreation.NodeCreationMessages;
import de.hpi.msc.jschneider.utility.Int64Range;
import lombok.val;
import org.ojalgo.function.aggregator.Aggregator;

import java.util.ArrayList;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class TestNodeCreationCoordinatorControl extends ProtocolTestCase
{
    private static final int NUMBER_OF_SAMPLES = 100;

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
        return new ProtocolType[]{ProtocolType.MessageExchange, ProtocolType.NodeCreation, ProtocolType.EdgeCreation};
    }

    private NodeCreationCoordinatorModel dummyModel()
    {
        return finalizeModel(NodeCreationCoordinatorModel.builder()
                                                         .totalNumberOfIntersectionSegments(NUMBER_OF_SAMPLES)
                                                         .build());
    }

    private NodeCreationCoordinatorControl control()
    {
        return new NodeCreationCoordinatorControl(dummyModel());
    }

    private void assertThatInitializationMessagesAreSent(TestProbe... participants)
    {
        val responsibilitiesCreatedEvent = expectEvent(NodeCreationEvents.ResponsibilitiesCreatedEvent.class);
        assertThat(responsibilitiesCreatedEvent.getSubSequenceResponsibilities().size()).isEqualTo(participants.length);
        assertThat(responsibilitiesCreatedEvent.getSegmentResponsibilities().size()).isEqualTo(participants.length);

        val initializeMessages = new ArrayList<NodeCreationMessages.InitializeNodeCreationMessage>();
        for (val participant : participants)
        {
            initializeMessages.add(localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(NodeCreationMessages.InitializeNodeCreationMessage.class));
        }

        for (val participant : participants)
        {
            val message = initializeMessages.stream().filter(m -> m.getReceiver().equals(participant.ref())).findFirst();
            assertThat(message.isPresent()).isTrue();
            assertThat(message.get().getSubSequenceResponsibilities()).isEqualTo(responsibilitiesCreatedEvent.getSubSequenceResponsibilities());
            assertThat(message.get().getIntersectionSegmentResponsibilities()).isEqualTo(responsibilitiesCreatedEvent.getSegmentResponsibilities());
        }

        assertThat(initializeMessages.stream().map(NodeCreationMessages.InitializeNodeCreationMessage::getMaximumValue).collect(Collectors.toSet())).hasSize(1);
    }

    public void testWaitForAllProcessorsToBeReady()
    {
        val control = control();
        val messageInterface = createMessageInterface(control);

        val firstReducedProjection = createMatrix(2, 100);
        val firstMaxValue = Math.max(firstReducedProjection.aggregateAll(Aggregator.MAXIMUM),
                                     -firstReducedProjection.aggregateAll(Aggregator.MINIMUM));
        val secondReducedProjection = createMatrix(2, 50);
        val secondMaxValue = Math.max(secondReducedProjection.aggregateAll(Aggregator.MAXIMUM),
                                      -secondReducedProjection.aggregateAll(Aggregator.MINIMUM));

        val firstWorkerReady = NodeCreationMessages.NodeCreationWorkerReadyMessage.builder()
                                                                                  .sender(self.ref())
                                                                                  .receiver(self.ref())
                                                                                  .isLastSubSequenceChunk(false)
                                                                                  .subSequenceIndices(Int64Range.builder()
                                                                                                                .from(0L)
                                                                                                                .to(firstReducedProjection.countColumns())
                                                                                                                .build())
                                                                                  .maximumValue(firstMaxValue)
                                                                                  .build();
        messageInterface.apply(firstWorkerReady);
        assertThat(control.getModel().getMaximumValue()).isEqualTo(firstMaxValue);
        assertThat(control.getModel().getReadyMessages().size()).isEqualTo(1);
        assertThat(control.getModel().getReadyMessages().get(0)).isEqualTo(firstWorkerReady);

        // do not initialize the node creation process yet
        assertThatMessageIsCompleted(firstWorkerReady);

        val secondWorkerReady = NodeCreationMessages.NodeCreationWorkerReadyMessage.builder()
                                                                                   .sender(remoteActor.ref())
                                                                                   .receiver(self.ref())
                                                                                   .isLastSubSequenceChunk(true)
                                                                                   .subSequenceIndices(Int64Range.builder()
                                                                                                                 .from(firstReducedProjection.countColumns())
                                                                                                                 .to(firstReducedProjection.countColumns() + secondReducedProjection.countColumns())
                                                                                                                 .build())
                                                                                   .maximumValue(secondMaxValue)
                                                                                   .build();
        messageInterface.apply(secondWorkerReady);
        assertThat(control.getModel().getMaximumValue()).isEqualTo(Math.max(firstMaxValue, secondMaxValue));
        assertThat(control.getModel().getReadyMessages().size()).isEqualTo(2);
        assertThat(control.getModel().getReadyMessages().get(1)).isEqualTo(secondWorkerReady);

        // initialize node creation process now that all processors reported ready
        assertThatInitializationMessagesAreSent(self, remoteActor);

        assertThatMessageIsCompleted(secondWorkerReady);
    }

    public void testOrderOfReadyMessagesDoesNotMatter()
    {
        val control = control();
        val messageInterface = createMessageInterface(control);

        val firstReducedProjection = createMatrix(2, 100);
        val firstMaxValue = Math.max(firstReducedProjection.aggregateAll(Aggregator.MAXIMUM),
                                     -firstReducedProjection.aggregateAll(Aggregator.MINIMUM));
        val secondReducedProjection = createMatrix(2, 50);
        val secondMaxValue = Math.max(secondReducedProjection.aggregateAll(Aggregator.MAXIMUM),
                                      -secondReducedProjection.aggregateAll(Aggregator.MINIMUM));

        val secondWorkerReady = NodeCreationMessages.NodeCreationWorkerReadyMessage.builder()
                                                                                   .sender(remoteActor.ref())
                                                                                   .receiver(self.ref())
                                                                                   .isLastSubSequenceChunk(true)
                                                                                   .subSequenceIndices(Int64Range.builder()
                                                                                                                 .from(firstReducedProjection.countColumns())
                                                                                                                 .to(firstReducedProjection.countColumns() + secondReducedProjection.countColumns())
                                                                                                                 .build())
                                                                                   .maximumValue(secondMaxValue)
                                                                                   .build();
        messageInterface.apply(secondWorkerReady);
        assertThat(control.getModel().getMaximumValue()).isEqualTo(secondMaxValue);
        assertThat(control.getModel().getReadyMessages().size()).isEqualTo(1);
        assertThat(control.getModel().getReadyMessages().get(0)).isEqualTo(secondWorkerReady);

        // do not initialize the node creation process yet
        assertThatMessageIsCompleted(secondWorkerReady);

        val firstWorkerReady = NodeCreationMessages.NodeCreationWorkerReadyMessage.builder()
                                                                                  .sender(self.ref())
                                                                                  .receiver(self.ref())
                                                                                  .isLastSubSequenceChunk(false)
                                                                                  .subSequenceIndices(Int64Range.builder()
                                                                                                                .from(0L)
                                                                                                                .to(firstReducedProjection.countColumns())
                                                                                                                .build())
                                                                                  .maximumValue(firstMaxValue)
                                                                                  .build();
        messageInterface.apply(firstWorkerReady);
        assertThat(control.getModel().getMaximumValue()).isEqualTo(Math.max(firstMaxValue, secondMaxValue));
        assertThat(control.getModel().getReadyMessages().size()).isEqualTo(2);
        assertThat(control.getModel().getReadyMessages().get(1)).isEqualTo(firstWorkerReady);

        // initialize node creation process now that all processors reported ready
        assertThatInitializationMessagesAreSent(self, remoteActor);

        assertThatMessageIsCompleted(firstWorkerReady);
    }
}
