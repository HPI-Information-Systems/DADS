package de.hpi.msc.jschneider.protocol.nodeCreation.worker;

import akka.actor.ActorRef;
import akka.testkit.TestProbe;
import de.hpi.msc.jschneider.protocol.ProtocolTestCase;
import de.hpi.msc.jschneider.protocol.TestProcessor;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.eventDispatcher.EventDispatcherMessages;
import de.hpi.msc.jschneider.protocol.dimensionReduction.DimensionReductionEvents;
import de.hpi.msc.jschneider.protocol.nodeCreation.NodeCreationEvents;
import de.hpi.msc.jschneider.protocol.nodeCreation.NodeCreationMessages;
import de.hpi.msc.jschneider.utility.Int32Range;
import lombok.val;
import lombok.var;
import org.ojalgo.function.aggregator.Aggregator;

import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;

public class TestNodeCreationWorkerControl extends ProtocolTestCase
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
        return new ProtocolType[]{ProtocolType.MessageExchange, ProtocolType.NodeCreation, ProtocolType.DimensionReduction};
    }

    private NodeCreationWorkerModel dummyModel()
    {
        return finalizeModel(NodeCreationWorkerModel.builder()
                                                    .build());
    }

    private NodeCreationWorkerControl control()
    {
        return new NodeCreationWorkerControl(dummyModel());
    }

    public void testSubscribeToReducedProjectionCreatedEvent()
    {
        val control = control();

        control.preStart();

        val subscription = localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(EventDispatcherMessages.SubscribeToEventMessage.class);
        assertThat(subscription.getEventType()).isEqualTo(DimensionReductionEvents.ReducedProjectionCreatedEvent.class);
    }

    public void testSendReadyOnReducedProjectionCreated()
    {
        val control = control();
        val messageInterface = createMessageInterface(control);

        val reducedProjection = createMatrix(2, 100);
        val maxValue = Math.max(reducedProjection.aggregateAll(Aggregator.MAXIMUM),
                                -reducedProjection.aggregateAll(Aggregator.MINIMUM));

        val reducedProjectionCreatedEvent = DimensionReductionEvents.ReducedProjectionCreatedEvent.builder()
                                                                                                  .sender(self.ref())
                                                                                                  .receiver(self.ref())
                                                                                                  .reducedProjection(reducedProjection)
                                                                                                  .firstSubSequenceIndex(0L)
                                                                                                  .isLastSubSequenceChunk(true)
                                                                                                  .build();
        messageInterface.apply(reducedProjectionCreatedEvent);

        assertThat(control.getModel().getReducedProjection().equals(reducedProjection, MATRIX_COMPARISON_CONTEXT)).isTrue();
        assertThat(control.getModel().getFirstSubSequenceIndex()).isEqualTo(0L);

        val readyMessage = localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(NodeCreationMessages.NodeCreationWorkerReadyMessage.class);
        assertThat(readyMessage.getSubSequenceIndices().getStart()).isEqualTo(0L);
        assertThat(readyMessage.getSubSequenceIndices().getEnd()).isEqualTo(reducedProjection.countColumns());
        assertThat(readyMessage.getMaximumValue()).isEqualTo(maxValue);

        assertThatMessageIsCompleted(reducedProjectionCreatedEvent);
    }

    public void testCalculateIntersectionsOnInitializeNodeCreation()
    {
        val control = control();
        val messageInterface = createMessageInterface(control);

        val reducedProjection = createMatrix(2, 100);
        val maxValue = Math.max(reducedProjection.aggregateAll(Aggregator.MAXIMUM),
                                -reducedProjection.aggregateAll(Aggregator.MINIMUM));

        control.getModel().setReducedProjection(reducedProjection);
        control.getModel().setFirstSubSequenceIndex(0L);
        control.getModel().setLastSubSequenceChunk(false);

        val responsibilities = new HashMap<ActorRef, Int32Range>();
        val numberOfSamples = 10;
        responsibilities.put(self.ref(), Int32Range.builder()
                                                   .start(0)
                                                   .end(numberOfSamples)
                                                   .build());

        val initializeNodeCreation = NodeCreationMessages.InitializeNodeCreationMessage.builder()
                                                                                       .sender(self.ref())
                                                                                       .receiver(self.ref())
                                                                                       .maximumValue(maxValue * 1.2d)
                                                                                       .numberOfSamples(numberOfSamples)
                                                                                       .sampleResponsibilities(responsibilities)
                                                                                       .build();
        messageInterface.apply(initializeNodeCreation);

        for (var i = 0; i < numberOfSamples; ++i)
        {
            val intersectionsAtAngle = localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(NodeCreationMessages.IntersectionsAtAngleMessage.class);
            assertThat(intersectionsAtAngle.getIntersectionPointIndex()).isEqualTo(i);
            assertThat(intersectionsAtAngle.getIntersections()).isNotNull();
        }

        assertThatMessageIsCompleted(initializeNodeCreation);
    }

    public void testWaitForAllIntersectionsToArrive()
    {
        val control = control();
        val messageInterface = createMessageInterface(control);

        val localProjection = createMatrix(2, 100);
        val localMax = (float) Math.max(localProjection.aggregateAll(Aggregator.MAXIMUM),
                                        -localProjection.aggregateAll(Aggregator.MINIMUM));
        val remoteProjection = createMatrix(2, 50);
        val remoteMax = (float) Math.max(remoteProjection.aggregateAll(Aggregator.MAXIMUM),
                                         -remoteProjection.aggregateAll(Aggregator.MINIMUM));
        val totalMax = Math.max(localMax, remoteMax);
        val numberOfSamples = 100;

        val responsibilities = new HashMap<ActorRef, Int32Range>();
        responsibilities.put(self.ref(), Int32Range.builder()
                                                   .start(0)
                                                   .end(numberOfSamples / 2)
                                                   .build());
        responsibilities.put(remoteActor.ref(), Int32Range.builder()
                                                          .start(numberOfSamples / 2)
                                                          .end(numberOfSamples)
                                                          .build());


        control.getModel().setMaximumValue(totalMax * 1.2d);
        control.getModel().setSampleResponsibilities(responsibilities);

        val intersectionPointIndex = 0;

        val localIntersection = NodeCreationMessages.IntersectionsAtAngleMessage.builder()
                                                                                .sender(self.ref())
                                                                                .receiver(self.ref())
                                                                                .intersectionPointIndex(intersectionPointIndex)
                                                                                .intersections(new float[]{0.0f * localMax, 0.15f * localMax, 0.25f * localMax, 0.33f * localMax, 0.45f * localMax, 0.75f * localMax, 0.99f * localMax})
                                                                                .build();
        messageInterface.apply(localIntersection);

        assertThat(control.getModel().getIntersections().get(intersectionPointIndex).get(0)).containsExactly(localIntersection.getIntersections());

        // do not extract any nodes yet
        assertThatMessageIsCompleted(localIntersection);

        val remoteIntersections = NodeCreationMessages.IntersectionsAtAngleMessage.builder()
                                                                                  .sender(remoteActor.ref())
                                                                                  .receiver(self.ref())
                                                                                  .intersectionPointIndex(intersectionPointIndex)
                                                                                  .intersections(new float[]{0.01f * remoteMax, 0.22f * remoteMax, 0.43f * remoteMax, 0.78f * remoteMax})
                                                                                  .build();
        messageInterface.apply(remoteIntersections);

        assertThat(control.getModel().getIntersections().get(intersectionPointIndex).get(1)).containsExactly(remoteIntersections.getIntersections());

        expectEvent(NodeCreationEvents.NodesCreatedEvent.class);

        assertThatMessageIsCompleted(remoteIntersections);
    }
}
