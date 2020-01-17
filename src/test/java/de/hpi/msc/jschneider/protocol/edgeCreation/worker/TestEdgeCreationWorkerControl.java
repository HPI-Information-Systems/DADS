package de.hpi.msc.jschneider.protocol.edgeCreation.worker;

import akka.actor.ActorRef;
import akka.testkit.TestProbe;
import de.hpi.msc.jschneider.math.Intersection;
import de.hpi.msc.jschneider.math.IntersectionCollection;
import de.hpi.msc.jschneider.protocol.ProtocolTestCase;
import de.hpi.msc.jschneider.protocol.TestProcessor;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.eventDispatcher.EventDispatcherMessages;
import de.hpi.msc.jschneider.protocol.nodeCreation.NodeCreationEvents;
import de.hpi.msc.jschneider.utility.Int32Range;
import de.hpi.msc.jschneider.utility.Int64Range;
import lombok.val;
import lombok.var;
import scala.PartialFunction;
import scala.runtime.BoxedUnit;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class TestEdgeCreationWorkerControl extends ProtocolTestCase
{
    private static final Random RANDOM = new Random();

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
        return new ProtocolType[]{ProtocolType.MessageExchange, ProtocolType.EdgeCreation, ProtocolType.NodeCreation};
    }

    private EdgeCreationWorkerModel dummyModel()
    {
        return finalizeModel(EdgeCreationWorkerModel.builder()
                                                    .build());
    }

    private EdgeCreationWorkerControl control()
    {
        return new EdgeCreationWorkerControl(dummyModel());
    }

    private Map<ActorRef, Int32Range> createIntersectionSegmentResponsibilities(int numberOfIntersectionSegments)
    {
        val responsibilities = new HashMap<ActorRef, Int32Range>();
        responsibilities.put(self.ref(), Int32Range.builder()
                                                   .from(0)
                                                   .to(numberOfIntersectionSegments / 2)
                                                   .build());
        responsibilities.put(remoteActor.ref(), Int32Range.builder()
                                                          .from(numberOfIntersectionSegments / 2)
                                                          .to(numberOfIntersectionSegments)
                                                          .build());

        return responsibilities;
    }

    private Map<ActorRef, Int64Range> createSubSequenceResponsibilities(long numberOfSubSequences)
    {
        val responsibilities = new HashMap<ActorRef, Int64Range>();
        responsibilities.put(self.ref(), Int64Range.builder()
                                                   .from(0L)
                                                   .to(numberOfSubSequences / 2)
                                                   .build());
        responsibilities.put(remoteActor.ref(), Int64Range.builder()
                                                          .from(numberOfSubSequences / 2)
                                                          .to(numberOfSubSequences)
                                                          .build());

        return responsibilities;
    }

    private IntersectionCollection[] createIntersectionCollections(int numberOfIntersectionSegments, Int64Range subSequenceRange)
    {
        val collections = new IntersectionCollection[numberOfIntersectionSegments];
        for (var segment = 0; segment < collections.length; ++segment)
        {
            val collection = IntersectionCollection.builder()
                                                   .intersectionSegment(segment)
                                                   .build();

            val usedSubSequenceIndices = new ArrayList<Long>();
            for (var intersectionIndex = 0; intersectionIndex < RANDOM.nextInt(20) + 10; ++intersectionIndex)
            {
                var subSequenceIndex = 0L;
                do
                {
                    subSequenceIndex = RANDOM.nextInt((int) subSequenceRange.length()) + subSequenceRange.getFrom();
                } while (usedSubSequenceIndices.contains(subSequenceIndex));

                usedSubSequenceIndices.add(subSequenceIndex);
                val intersectionDistance = RANDOM.nextFloat() * 100.0f;

                collection.getIntersections().add(Intersection.builder()
                                                              .subSequenceIndex(subSequenceIndex)
                                                              .intersectionDistance(intersectionDistance)
                                                              .build());
            }

            assertThat(collection.getIntersections().stream().map(Intersection::getSubSequenceIndex).collect(Collectors.toSet()))
                    .containsExactlyInAnyOrder(collection.getIntersections().stream().map(Intersection::getSubSequenceIndex).toArray(Long[]::new));

            collections[segment] = collection;
        }

        return collections;
    }

    private void assertThatQueueIsSorted(Queue<LocalIntersection> queue)
    {
        var lastSubSequenceIndex = -1L;
        var lastIntersectionSegment = -1;
        while (!queue.isEmpty())
        {
            val intersection = queue.poll();

            assertThat(intersection).isNotNull();
            assertThat(intersection.getSubSequenceIndex() > lastSubSequenceIndex ||
                       (intersection.getSubSequenceIndex() == lastSubSequenceIndex && intersection.getIntersectionSegment() > lastIntersectionSegment))
                    .isTrue();

            lastSubSequenceIndex = intersection.getSubSequenceIndex();
            lastIntersectionSegment = intersection.getIntersectionSegment();
        }
    }

    private NodeCreationEvents.IntersectionsCalculatedEvent sendIntersections(EdgeCreationWorkerControl control,
                                                                              PartialFunction<Object, BoxedUnit> messageInterface,
                                                                              IntersectionCollection[] intersectionCollections,
                                                                              boolean expectLastMessageToBeCompleted)
    {
        NodeCreationEvents.IntersectionsCalculatedEvent message = null;
        for (var i = 0; i < intersectionCollections.length; ++i)
        {
            assertThat(control.getModel().getIntersectionsInSegment().get(i)).isNull();

            message = NodeCreationEvents.IntersectionsCalculatedEvent.builder()
                                                                     .sender(self.ref())
                                                                     .receiver(self.ref())
                                                                     .intersectionCollection(intersectionCollections[i])
                                                                     .build();
            messageInterface.apply(message);

            assertThat(control.getModel().getIntersectionsInSegment().get(i)).isNotNull();

            val isLastCollection = i == intersectionCollections.length - 1;
            if (!isLastCollection || expectLastMessageToBeCompleted)
            {
                assertThatMessageIsCompleted(message);
            }
        }

        return message;
    }

    public void testSubscribeToEvents()
    {
        val control = control();
        control.preStart();

        val responsibilitiesReceivedSubscription = localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(EventDispatcherMessages.SubscribeToEventMessage.class);
        assertThat(responsibilitiesReceivedSubscription.getEventType()).isEqualTo(NodeCreationEvents.ResponsibilitiesReceivedEvent.class);

        val intersectionsCalculatedSubscription = localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(EventDispatcherMessages.SubscribeToEventMessage.class);
        assertThat(intersectionsCalculatedSubscription.getEventType()).isEqualTo(NodeCreationEvents.IntersectionsCalculatedEvent.class);
    }

    public void testWaitForResponsibilitiesAndLocalIntersectionsBeforeCreatingQueue1()
    {
        val control = control();
        val messageInterface = createMessageInterface(control);

        val numberOfIntersectionSegments = 4;
        val numberOfSubSequences = 200;
        val segmentResponsibilities = createIntersectionSegmentResponsibilities(numberOfIntersectionSegments);
        val sequenceResponsibilities = createSubSequenceResponsibilities(numberOfSubSequences);

        val responsibilitiesMessage = NodeCreationEvents.ResponsibilitiesReceivedEvent.builder()
                                                                                      .sender(self.ref())
                                                                                      .receiver(self.ref())
                                                                                      .segmentResponsibilities(segmentResponsibilities.get(self.ref()))
                                                                                      .subSequenceResponsibilities(sequenceResponsibilities.get(self.ref()))
                                                                                      .numberOfIntersectionSegments(numberOfIntersectionSegments)
                                                                                      .build();
        messageInterface.apply(responsibilitiesMessage);

        assertThat(control.getModel().getIntersectionsToMatch()).isNull();
        assertThatMessageIsCompleted(responsibilitiesMessage);

        val intersections = createIntersectionCollections(numberOfIntersectionSegments, sequenceResponsibilities.get(self.ref()));
        val totalNumberOfIntersections = Arrays.stream(intersections).mapToLong(intersection -> intersection.getIntersections().size()).sum();

        val lastIntersectionsCreatedEvent = sendIntersections(control, messageInterface, intersections, false);

        assertThat(control.getModel().getIntersectionsToMatch().size()).isEqualTo(totalNumberOfIntersections);
        assertThatQueueIsSorted(control.getModel().getIntersectionsToMatch());

        assertThatMessageIsCompleted(lastIntersectionsCreatedEvent);
    }

    public void testWaitForResponsibilitiesAndLocalIntersectionsBeforeCreatingQueue2()
    {
        val control = control();
        val messageInterface = createMessageInterface(control);

        val numberOfIntersectionSegments = 4;
        val numberOfSubSequences = 200;
        val segmentResponsibilities = createIntersectionSegmentResponsibilities(numberOfIntersectionSegments);
        val sequenceResponsibilities = createSubSequenceResponsibilities(numberOfSubSequences);

        val intersections = createIntersectionCollections(numberOfIntersectionSegments, sequenceResponsibilities.get(self.ref()));
        val totalNumberOfIntersections = Arrays.stream(intersections).mapToLong(intersection -> intersection.getIntersections().size()).sum();

        sendIntersections(control, messageInterface, intersections, true);

        val responsibilitiesMessage = NodeCreationEvents.ResponsibilitiesReceivedEvent.builder()
                                                                                      .sender(self.ref())
                                                                                      .receiver(self.ref())
                                                                                      .segmentResponsibilities(segmentResponsibilities.get(self.ref()))
                                                                                      .subSequenceResponsibilities(sequenceResponsibilities.get(self.ref()))
                                                                                      .numberOfIntersectionSegments(numberOfIntersectionSegments)
                                                                                      .build();
        messageInterface.apply(responsibilitiesMessage);

        assertThat(control.getModel().getIntersectionsToMatch().size()).isEqualTo(totalNumberOfIntersections);
        assertThatQueueIsSorted(control.getModel().getIntersectionsToMatch());

        assertThatMessageIsCompleted(responsibilitiesMessage);
    }


}
