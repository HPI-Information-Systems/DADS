package de.hpi.msc.jschneider.protocol.nodeCreation.worker;

import akka.actor.ActorRef;
import akka.testkit.TestProbe;
import com.google.common.primitives.Floats;
import de.hpi.msc.jschneider.math.Intersection;
import de.hpi.msc.jschneider.protocol.ProtocolTestCase;
import de.hpi.msc.jschneider.protocol.TestProcessor;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.eventDispatcher.EventDispatcherMessages;
import de.hpi.msc.jschneider.protocol.dimensionReduction.DimensionReductionEvents;
import de.hpi.msc.jschneider.protocol.nodeCreation.NodeCreationEvents;
import de.hpi.msc.jschneider.protocol.nodeCreation.NodeCreationMessages;
import de.hpi.msc.jschneider.utility.Int32Range;
import de.hpi.msc.jschneider.utility.Int64Range;
import lombok.val;
import lombok.var;
import org.assertj.core.data.Offset;
import org.ojalgo.function.aggregator.Aggregator;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

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
        return new ProtocolType[]{ProtocolType.MessageExchange, ProtocolType.NodeCreation, ProtocolType.DimensionReduction, ProtocolType.EdgeCreation};
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
        assertThat(readyMessage.getSubSequenceIndices().getFrom()).isEqualTo(0L);
        assertThat(readyMessage.getSubSequenceIndices().getTo()).isEqualTo(reducedProjection.countColumns());
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
        control.getModel().setLastSubSequenceChunk(true);

        val intersectionSegmentResponsibilities = new HashMap<ActorRef, Int32Range>();
        val numberOfIntersectionSegments = 10;
        intersectionSegmentResponsibilities.put(self.ref(), Int32Range.builder()
                                                                      .from(0)
                                                                      .to(numberOfIntersectionSegments)
                                                                      .build());
        val subSequenceResponsibilities = new HashMap<ActorRef, Int64Range>();
        subSequenceResponsibilities.put(self.ref(), Int64Range.builder()
                                                              .from(0L)
                                                              .to(reducedProjection.countColumns())
                                                              .build());

        val initializeNodeCreation = NodeCreationMessages.InitializeNodeCreationMessage.builder()
                                                                                       .sender(self.ref())
                                                                                       .receiver(self.ref())
                                                                                       .maximumValue(maxValue * 1.2d)
                                                                                       .numberOfIntersectionSegments(numberOfIntersectionSegments)
                                                                                       .intersectionSegmentResponsibilities(intersectionSegmentResponsibilities)
                                                                                       .subSequenceResponsibilities(subSequenceResponsibilities)
                                                                                       .build();
        messageInterface.apply(initializeNodeCreation);

        val responsibilitiesReceivedEvent = expectEvent(NodeCreationEvents.ResponsibilitiesReceivedEvent.class);
        assertThat(responsibilitiesReceivedEvent.getSegmentResponsibilities()).isEqualTo(intersectionSegmentResponsibilities.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().path().root(), Map.Entry::getValue)));
        assertThat(responsibilitiesReceivedEvent.getSubSequenceResponsibilities()).isEqualTo(subSequenceResponsibilities.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().path().root(), Map.Entry::getValue)));
        assertThat(responsibilitiesReceivedEvent.getNumberOfIntersectionSegments()).isEqualTo(numberOfIntersectionSegments);

        for (var i = 0; i < numberOfIntersectionSegments; ++i)
        {
            val intersectionsAtAngle = localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(NodeCreationMessages.IntersectionsMessage.class);
            assertThat(intersectionsAtAngle.getIntersectionSegment()).isEqualTo(i);
            assertThat(intersectionsAtAngle.getIntersections()).isNotNull();

            val intersectionsCalculatedEvent = expectEvent(NodeCreationEvents.IntersectionsCalculatedEvent.class);
            assertThat(intersectionsCalculatedEvent.getIntersectionCollection().getIntersectionSegment()).isEqualTo(i);
            assertThat(intersectionsCalculatedEvent.getIntersectionCollection().getIntersections()).isNotNull();

            assertThat(Floats.toArray(intersectionsCalculatedEvent.getIntersectionCollection().getIntersections().stream().map(Intersection::getIntersectionDistance).collect(Collectors.toList())))
                    .containsExactly(intersectionsAtAngle.getIntersections());
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
        val numberOfIntersectionSegments = 100;

        val intersectionSegmentResponsibilities = new HashMap<ActorRef, Int32Range>();
        intersectionSegmentResponsibilities.put(self.ref(), Int32Range.builder()
                                                                      .from(0)
                                                                      .to(numberOfIntersectionSegments / 2)
                                                                      .build());
        intersectionSegmentResponsibilities.put(remoteActor.ref(), Int32Range.builder()
                                                                             .from(numberOfIntersectionSegments / 2)
                                                                             .to(numberOfIntersectionSegments)
                                                                             .build());


        control.getModel().setFirstSubSequenceIndex(0L);
        control.getModel().setMaximumValue(totalMax * 1.2d);
        control.getModel().setLastSubSequenceChunk(false);
        control.getModel().setIntersectionSegmentResponsibilities(intersectionSegmentResponsibilities);

        val intersectionPointIndex = 0;

        val localIntersection = NodeCreationMessages.IntersectionsMessage.builder()
                                                                         .sender(self.ref())
                                                                         .receiver(self.ref())
                                                                         .intersectionSegment(intersectionPointIndex)
                                                                         .intersections(new float[]{0.0f * localMax, 0.15f * localMax, 0.25f * localMax, 0.33f * localMax, 0.45f * localMax, 0.75f * localMax, 0.99f * localMax})
                                                                         .build();
        messageInterface.apply(localIntersection);

        assertThat(control.getModel().getIntersections().get(intersectionPointIndex).get(0)).containsExactly(localIntersection.getIntersections());

        // do not extract any nodes yet
        assertThatMessageIsCompleted(localIntersection);

        val remoteIntersections = NodeCreationMessages.IntersectionsMessage.builder()
                                                                           .sender(remoteActor.ref())
                                                                           .receiver(self.ref())
                                                                           .intersectionSegment(intersectionPointIndex)
                                                                           .intersections(new float[]{0.01f * remoteMax, 0.22f * remoteMax, 0.43f * remoteMax, 0.78f * remoteMax})
                                                                           .build();
        messageInterface.apply(remoteIntersections);

        assertThat(control.getModel().getIntersections().get(intersectionPointIndex).get(1)).containsExactly(remoteIntersections.getIntersections());

        localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(NodeCreationMessages.NodesMessage.class);
        localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(NodeCreationMessages.NodesMessage.class);

        assertThatMessageIsCompleted(remoteIntersections);
    }

    public void testSendReducedSubSequence()
    {
        val tolerance = Offset.offset(0.00001f);

        val control = control();
        val messageInterface = createMessageInterface(control);

        val reducedProjection = createMatrix(2, 100);
        control.getModel().setFirstSubSequenceIndex(0L);
        control.getModel().setLastSubSequenceChunk(false);
        control.getModel().setReducedProjection(reducedProjection);

        val numberOfIntersectionSegments = 100;
        val segmentResponsibilities = new HashMap<ActorRef, Int32Range>();
        segmentResponsibilities.put(self.ref(), Int32Range.builder()
                                                          .from(0)
                                                          .to(numberOfIntersectionSegments / 2)
                                                          .build());
        segmentResponsibilities.put(remoteActor.ref(), Int32Range.builder()
                                                                 .from(numberOfIntersectionSegments / 2)
                                                                 .to(numberOfIntersectionSegments)
                                                                 .build());

        val subSequenceResponsibilities = new HashMap<ActorRef, Int64Range>();
        subSequenceResponsibilities.put(self.ref(), Int64Range.builder()
                                                              .from(0L)
                                                              .to(reducedProjection.countColumns())
                                                              .build());
        subSequenceResponsibilities.put(remoteActor.ref(), Int64Range.builder()
                                                                     .from(reducedProjection.countColumns())
                                                                     .to(reducedProjection.countColumns() * 2)
                                                                     .build());

        val initializeMessage = NodeCreationMessages.InitializeNodeCreationMessage.builder()
                                                                                  .sender(self.ref())
                                                                                  .receiver(self.ref())
                                                                                  .maximumValue(0.0d)
                                                                                  .numberOfIntersectionSegments(numberOfIntersectionSegments)
                                                                                  .intersectionSegmentResponsibilities(segmentResponsibilities)
                                                                                  .subSequenceResponsibilities(subSequenceResponsibilities)
                                                                                  .build();
        messageInterface.apply(initializeMessage);

        val responsibilitiesReceivedEvent = expectEvent(NodeCreationEvents.ResponsibilitiesReceivedEvent.class);
        assertThat(responsibilitiesReceivedEvent.getSegmentResponsibilities()).isEqualTo(segmentResponsibilities.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().path().root(), Map.Entry::getValue)));
        assertThat(responsibilitiesReceivedEvent.getSubSequenceResponsibilities()).isEqualTo(subSequenceResponsibilities.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().path().root(), Map.Entry::getValue)));

        val reducedSubSequenceMessage = localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(NodeCreationMessages.ReducedSubSequenceMessage.class);
        assertThat(reducedSubSequenceMessage.getReceiver()).isEqualTo(remoteActor.ref());
        assertThat(reducedSubSequenceMessage.getSubSequenceIndex()).isEqualTo(reducedProjection.countColumns());
        assertThat(reducedSubSequenceMessage.getSubSequenceX()).isCloseTo(reducedProjection.get(0L, reducedProjection.countColumns() - 1).floatValue(), tolerance);
        assertThat(reducedSubSequenceMessage.getSubSequenceY()).isCloseTo(reducedProjection.get(1L, reducedProjection.countColumns() - 1).floatValue(), tolerance);
    }
}
