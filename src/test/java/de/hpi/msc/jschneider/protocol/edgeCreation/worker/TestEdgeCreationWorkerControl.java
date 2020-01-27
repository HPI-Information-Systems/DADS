package de.hpi.msc.jschneider.protocol.edgeCreation.worker;

import akka.testkit.TestProbe;
import com.google.common.primitives.Floats;
import de.hpi.msc.jschneider.data.graph.GraphEdge;
import de.hpi.msc.jschneider.math.Intersection;
import de.hpi.msc.jschneider.math.IntersectionCollection;
import de.hpi.msc.jschneider.math.Node;
import de.hpi.msc.jschneider.math.NodeCollection;
import de.hpi.msc.jschneider.protocol.ProtocolTestCase;
import de.hpi.msc.jschneider.protocol.TestProcessor;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.eventDispatcher.EventDispatcherMessages;
import de.hpi.msc.jschneider.protocol.edgeCreation.EdgeCreationEvents;
import de.hpi.msc.jschneider.protocol.nodeCreation.NodeCreationEvents;
import de.hpi.msc.jschneider.protocol.nodeCreation.NodeCreationMessages;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorId;
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

    private Map<ProcessorId, Int32Range> createIntersectionSegmentResponsibilities(int numberOfIntersectionSegments)
    {
        val responsibilities = new HashMap<ProcessorId, Int32Range>();
        responsibilities.put(ProcessorId.of(self.ref()), Int32Range.builder()
                                                                   .from(0)
                                                                   .to(numberOfIntersectionSegments / 2)
                                                                   .build());
        responsibilities.put(ProcessorId.of(remoteActor.ref()), Int32Range.builder()
                                                                          .from(numberOfIntersectionSegments / 2)
                                                                          .to(numberOfIntersectionSegments)
                                                                          .build());

        return responsibilities;
    }

    private Map<ProcessorId, Int64Range> createSubSequenceResponsibilities(long numberOfSubSequences)
    {
        val responsibilities = new HashMap<ProcessorId, Int64Range>();
        responsibilities.put(ProcessorId.of(self.ref()), Int64Range.builder()
                                                                   .from(0L)
                                                                   .to(numberOfSubSequences / 2)
                                                                   .build());
        responsibilities.put(ProcessorId.of(remoteActor.ref()), Int64Range.builder()
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

    private NodeCreationMessages.NodesMessage sendNodes(EdgeCreationWorkerControl control,
                                                        PartialFunction<Object, BoxedUnit> messageInterface,
                                                        NodeCollection nodeCollection,
                                                        boolean expectMessageToBeCompleted)
    {
        assertThat(control.getModel().getNodesInSegment()).doesNotContainKeys(nodeCollection.getIntersectionSegment());

        val nodes = nodeCollection.getNodes().stream().map(Node::getIntersectionLength).collect(Collectors.toList());
        val message = NodeCreationMessages.NodesMessage.builder()
                                                       .sender(self.ref())
                                                       .receiver(self.ref())
                                                       .intersectionSegment(nodeCollection.getIntersectionSegment())
                                                       .nodes(Floats.toArray(nodes))
                                                       .build();
        messageInterface.apply(message);

        assertThat(control.getModel().getNodesInSegment()).containsKeys(nodeCollection.getIntersectionSegment());

        if (!expectMessageToBeCompleted)
        {
            return message;
        }

        assertThatMessageIsCompleted(message);
        return message;
    }

    private NodeCollection createNodeCollection(int intersectionSegment, float... nodeIntersectionLengths)
    {
        val collection = NodeCollection.builder()
                                       .intersectionSegment(intersectionSegment)
                                       .build();
        for (val intersectionLength : nodeIntersectionLengths)
        {
            collection.getNodes().add(Node.builder()
                                          .intersectionLength(intersectionLength)
                                          .build());
        }

        return collection;
    }

    private IntersectionCollection createIntersectionCollection(int intersectionSegment, Intersection... intersections)
    {
        val collection = IntersectionCollection.builder()
                                               .intersectionSegment(intersectionSegment)
                                               .build();
        for (val intersection : intersections)
        {
            collection.getIntersections().add(intersection);
        }

        return collection;
    }

    private Intersection createIntersection(long subSequenceIndex, float intersectionLength)
    {
        return Intersection.builder()
                           .subSequenceIndex(subSequenceIndex)
                           .intersectionDistance(intersectionLength)
                           .build();
    }

    private IntersectionCollection[] extractLocalIntersectionCollections(IntersectionCollection[] allIntersectionCollections, Int64Range subSequenceResponsibility)
    {
        val collections = new IntersectionCollection[allIntersectionCollections.length];
        for (var segment = 0; segment < collections.length; ++segment)
        {
            val collection = IntersectionCollection.builder()
                                                   .intersectionSegment(segment)
                                                   .build();
            collections[segment] = collection;

            for (val intersection : allIntersectionCollections[segment].getIntersections())
            {
                if (!subSequenceResponsibility.contains(intersection.getSubSequenceIndex()))
                {
                    continue;
                }

                collection.getIntersections().add(intersection);
            }
        }

        return collections;
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
                                                                                      .segmentResponsibilities(segmentResponsibilities)
                                                                                      .subSequenceResponsibilities(sequenceResponsibilities)
                                                                                      .numberOfIntersectionSegments(numberOfIntersectionSegments)
                                                                                      .build();
        messageInterface.apply(responsibilitiesMessage);

        assertThat(control.getModel().getIntersectionsToMatch()).isNull();
        assertThatMessageIsCompleted(responsibilitiesMessage);

        val intersections = createIntersectionCollections(numberOfIntersectionSegments, sequenceResponsibilities.get(ProcessorId.of(self.ref())));
        val totalNumberOfIntersections = Arrays.stream(intersections).mapToLong(collection -> collection.getIntersections().size()).sum();

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

        val intersections = createIntersectionCollections(numberOfIntersectionSegments, sequenceResponsibilities.get(ProcessorId.of(self.ref())));
        val totalNumberOfIntersections = Arrays.stream(intersections).mapToLong(collection -> collection.getIntersections().size()).sum();

        sendIntersections(control, messageInterface, intersections, true);

        val responsibilitiesMessage = NodeCreationEvents.ResponsibilitiesReceivedEvent.builder()
                                                                                      .sender(self.ref())
                                                                                      .receiver(self.ref())
                                                                                      .segmentResponsibilities(segmentResponsibilities)
                                                                                      .subSequenceResponsibilities(sequenceResponsibilities)
                                                                                      .numberOfIntersectionSegments(numberOfIntersectionSegments)
                                                                                      .build();
        messageInterface.apply(responsibilitiesMessage);

        assertThat(control.getModel().getIntersectionsToMatch().size()).isEqualTo(totalNumberOfIntersections);
        assertThatQueueIsSorted(control.getModel().getIntersectionsToMatch());

        assertThatMessageIsCompleted(responsibilitiesMessage);
    }

    public void testCreateEdgesGradually()
    {
        val control = control();
        val messageInterface = createMessageInterface(control);

        val numberOfIntersectionSegments = 8;
        val numberOfSubSequences = 16;
        val segmentResponsibilities = createIntersectionSegmentResponsibilities(numberOfIntersectionSegments);
        val sequenceResponsibilities = createSubSequenceResponsibilities(numberOfSubSequences);

        val nodeCollections = new NodeCollection[numberOfIntersectionSegments];
        nodeCollections[0] = createNodeCollection(0, 1.5f, 3.5f);
        nodeCollections[1] = createNodeCollection(1, 3.0f, 4.5f);
        nodeCollections[2] = createNodeCollection(2, 3.5f);
        nodeCollections[3] = createNodeCollection(3, 1.0f, 4.0f);
        nodeCollections[4] = createNodeCollection(4, 5.0f);
        nodeCollections[5] = createNodeCollection(5, 3.0f);
        nodeCollections[6] = createNodeCollection(6, 2.5f, 4.5f);
        nodeCollections[7] = createNodeCollection(7, 2.0f, 3.0f);

        val intersectionCollections = new IntersectionCollection[numberOfIntersectionSegments];
        intersectionCollections[2] = createIntersectionCollection(2,
                                                                  createIntersection(0L, 3.5f));
        intersectionCollections[1] = createIntersectionCollection(1,
                                                                  createIntersection(1L, 3.0f),
                                                                  createIntersection(2L, 4.5f),
                                                                  createIntersection(3L, 3.0f),
                                                                  createIntersection(4L, 4.5f));
        intersectionCollections[3] = createIntersectionCollection(3,
                                                                  createIntersection(5L, 4.0f),
                                                                  createIntersection(7L, 1.0f));
        intersectionCollections[4] = createIntersectionCollection(4,
                                                                  createIntersection(8L, 5.0f));
        intersectionCollections[5] = createIntersectionCollection(5,
                                                                  createIntersection(9L, 3.0f));
        intersectionCollections[0] = createIntersectionCollection(0,
                                                                  createIntersection(10L, 1.5f),
                                                                  createIntersection(15L, 3.5f));
        intersectionCollections[6] = createIntersectionCollection(6,
                                                                  createIntersection(11L, 2.5f),
                                                                  createIntersection(14L, 4.5f));
        intersectionCollections[7] = createIntersectionCollection(7,
                                                                  createIntersection(12L, 3.0f),
                                                                  createIntersection(13L, 2.0f));

        val localIntersectionCollections = extractLocalIntersectionCollections(intersectionCollections, sequenceResponsibilities.get(ProcessorId.of(self.ref())));
        val totalNumberOfLocalIntersections = Arrays.stream(localIntersectionCollections).mapToLong(collection -> collection.getIntersections().size()).sum();
        sendIntersections(control, messageInterface, localIntersectionCollections, true);

        val responsibilitiesMessage = NodeCreationEvents.ResponsibilitiesReceivedEvent.builder()
                                                                                      .sender(self.ref())
                                                                                      .receiver(self.ref())
                                                                                      .segmentResponsibilities(segmentResponsibilities)
                                                                                      .subSequenceResponsibilities(sequenceResponsibilities)
                                                                                      .numberOfIntersectionSegments(numberOfIntersectionSegments)
                                                                                      .build();
        messageInterface.apply(responsibilitiesMessage);

        assertThat(control.getModel().getIntersectionsToMatch().size()).isEqualTo(totalNumberOfLocalIntersections);
        assertThatMessageIsCompleted(responsibilitiesMessage);

        sendNodes(control, messageInterface, nodeCollections[0], true);
        assertThat(control.getModel().getGraph().getEdges()).isEmpty();

        sendNodes(control, messageInterface, nodeCollections[1], true);
        assertThat(control.getModel().getGraph().getEdges()).isEmpty();

        sendNodes(control, messageInterface, nodeCollections[2], true);
        assertThat(control.getModel().getGraph().getEdges().values().stream().map(GraphEdge::toString).collect(Collectors.toList()))
                .containsExactlyInAnyOrder("{2_0} -[1]-> {1_0}",
                                           "{1_0} -[2]-> {1_1}",
                                           "{1_1} -[1]-> {1_0}");

        val nodesMessage = sendNodes(control, messageInterface, nodeCollections[3], false);
        assertThat(control.getModel().getGraph().getEdges().values().stream().map(GraphEdge::toString).collect(Collectors.toList()))
                .containsExactlyInAnyOrder("{2_0} -[1]-> {1_0}",
                                           "{1_0} -[2]-> {1_1}",
                                           "{1_1} -[1]-> {1_0}",
                                           "{1_1} -[1]-> {3_1}",
                                           "{3_1} -[1]-> {3_1}",
                                           "{3_1} -[1]-> {3_0}");
        assertThat(control.getModel().getIntersectionsToMatch()).isEmpty();

        val partitionCreatedEvent = expectEvent(EdgeCreationEvents.LocalGraphPartitionCreatedEvent.class);
        assertThat(partitionCreatedEvent.getGraphPartition().getEdges().values()).containsExactlyInAnyOrder(control.getModel().getGraph().getEdges().values().toArray(new GraphEdge[0]));
        assertThat(partitionCreatedEvent.getGraphPartition().getCreatedEdgesBySubSequenceIndex().size()).isEqualTo(control.getModel().getGraph().getEdges().values().stream().mapToLong(GraphEdge::getWeight).sum());

        assertThatMessageIsCompleted(nodesMessage);
    }
}