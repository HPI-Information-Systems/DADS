package de.hpi.msc.jschneider.protocol.nodeCreation.worker;

import akka.actor.ActorRef;
import com.google.common.primitives.Doubles;
import de.hpi.msc.jschneider.math.Calculate;
import de.hpi.msc.jschneider.math.Intersection;
import de.hpi.msc.jschneider.math.IntersectionCollection;
import de.hpi.msc.jschneider.math.Node;
import de.hpi.msc.jschneider.math.NodeCollection;
import de.hpi.msc.jschneider.math.kernelDensity.GaussianKernelDensity;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.dimensionReduction.DimensionReductionEvents;
import de.hpi.msc.jschneider.protocol.nodeCreation.NodeCreationEvents;
import de.hpi.msc.jschneider.protocol.nodeCreation.NodeCreationMessages;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorId;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import de.hpi.msc.jschneider.utility.Int64Range;
import de.hpi.msc.jschneider.utility.MatrixInitializer;
import lombok.val;
import lombok.var;
import org.ojalgo.function.aggregator.Aggregator;

import java.util.ArrayList;
import java.util.Map;
import java.util.stream.Collectors;

public class NodeCreationWorkerControl extends AbstractProtocolParticipantControl<NodeCreationWorkerModel>
{
    private static final int NUMBER_OF_DENSITY_SAMPLES = 250;
    private static final double DENSITY_SAMPLES_SCALING_FACTOR = 1.2d;

    public NodeCreationWorkerControl(NodeCreationWorkerModel model)
    {
        super(model);
    }

    @Override
    public ImprovedReceiveBuilder complementReceiveBuilder(ImprovedReceiveBuilder builder)
    {
        return super.complementReceiveBuilder(builder)
                    .match(DimensionReductionEvents.ReducedProjectionCreatedEvent.class, this::onReducedProjectionCreated)
                    .match(NodeCreationMessages.InitializeNodeCreationMessage.class, this::onInitializeNodeCreation)
                    .match(NodeCreationMessages.ReducedSubSequenceMessage.class, this::onReducedSubSequence)
                    .match(NodeCreationMessages.IntersectionsMessage.class, this::onIntersections);
    }

    @Override
    public void preStart()
    {
        super.preStart();

        subscribeToLocalEvent(ProtocolType.DimensionReduction, DimensionReductionEvents.ReducedProjectionCreatedEvent.class);
    }

    private void onReducedProjectionCreated(DimensionReductionEvents.ReducedProjectionCreatedEvent message)
    {
        try
        {
            getModel().setReducedProjection(message.getReducedProjection());
            getModel().setFirstSubSequenceIndex(message.getFirstSubSequenceIndex());
            getModel().setLastSubSequenceChunk(message.isLastSubSequenceChunk());
            val maximumValue = Math.max(getModel().getReducedProjection().aggregateAll(Aggregator.MAXIMUM),
                                        -getModel().getReducedProjection().aggregateAll(Aggregator.MINIMUM));
            val subSequenceIndices = Int64Range.builder()
                                               .from(getModel().getFirstSubSequenceIndex())
                                               .to(getModel().getFirstSubSequenceIndex() + getModel().getReducedProjection().countColumns())
                                               .build();

            val protocol = getMasterProtocol(ProtocolType.NodeCreation);
            if (!protocol.isPresent())
            {
                getLog().error("Unable to perform three-way-handshake, because master protocol (NodeCreationCoordinator) is missing!");
                return;
            }

            send(NodeCreationMessages.NodeCreationWorkerReadyMessage.builder()
                                                                    .sender(getModel().getSelf())
                                                                    .receiver(protocol.get().getRootActor())
                                                                    .subSequenceIndices(subSequenceIndices)
                                                                    .maximumValue(maximumValue)
                                                                    .isLastSubSequenceChunk(getModel().isLastSubSequenceChunk())
                                                                    .build());
        }
        finally
        {
            complete(message);
        }
    }

    private void onInitializeNodeCreation(NodeCreationMessages.InitializeNodeCreationMessage message)
    {
        try
        {
            getModel().setIntersectionSegmentResponsibilities(message.getIntersectionSegmentResponsibilities());
            getModel().setSubSequenceResponsibilities(message.getSubSequenceResponsibilities());
            getModel().setMaximumValue(message.getMaximumValue());
            getModel().setNumberOfIntersectionSegments(message.getNumberOfIntersectionSegments());

            val transformedSubSequenceResponsibilities = message.getSubSequenceResponsibilities().entrySet().stream().collect(Collectors.toMap(e -> ProcessorId.of(e.getKey()), Map.Entry::getValue));
            val transformedSegmentResponsibilities = message.getIntersectionSegmentResponsibilities().entrySet().stream().collect(Collectors.toMap(e -> ProcessorId.of(e.getKey()), Map.Entry::getValue));

            trySendEvent(ProtocolType.NodeCreation, eventDispatcher -> NodeCreationEvents.ResponsibilitiesReceivedEvent.builder()
                                                                                                                       .sender(getModel().getSelf())
                                                                                                                       .receiver(eventDispatcher)
                                                                                                                       .segmentResponsibilities(transformedSegmentResponsibilities)
                                                                                                                       .subSequenceResponsibilities(transformedSubSequenceResponsibilities)
                                                                                                                       .numberOfIntersectionSegments(message.getNumberOfIntersectionSegments())
                                                                                                                       .build());

            sendReducedSubSequenceToNextProcessor();
            calculateIntersections();
        }
        finally
        {
            complete(message);
        }
    }

    private void sendReducedSubSequenceToNextProcessor()
    {
        if (getModel().isLastSubSequenceChunk())
        {
            // there is no next processor
            return;
        }

        val subSequenceIndexToSend = getModel().getFirstSubSequenceIndex() + getModel().getReducedProjection().countColumns();
        for (val entry : getModel().getSubSequenceResponsibilities().entrySet())
        {
            val subSequenceIndices = entry.getValue();
            if (!subSequenceIndices.contains(subSequenceIndexToSend))
            {
                continue;
            }

            val receiver = entry.getKey();
            val columnIndex = getModel().getReducedProjection().countColumns() - 1;
            val x = getModel().getReducedProjection().get(0L, columnIndex);
            val y = getModel().getReducedProjection().get(1L, columnIndex);
            send(NodeCreationMessages.ReducedSubSequenceMessage.builder()
                                                               .sender(getModel().getSelf())
                                                               .receiver(receiver)
                                                               .subSequenceIndex(subSequenceIndexToSend)
                                                               .subSequenceX(x)
                                                               .subSequenceY(y)
                                                               .build());
            return;
        }

        getLog().error(String.format("Unable to find responsible actor for subsequence index %1$d!", subSequenceIndexToSend));
    }

    private void onReducedSubSequence(NodeCreationMessages.ReducedSubSequenceMessage message)
    {
        try
        {
            assert getModel().getReducedSubSequenceMessage() == null : "Already received a ReducedSubSequence message!";
            getModel().setReducedSubSequenceMessage(message);

            calculateIntersections();
        }
        finally
        {
            complete(message);
        }
    }

    private void calculateIntersections()
    {
        if (!isReadyToCalculateIntersections())
        {
            return;
        }

        var projection = getModel().getReducedProjection();
        if (getModel().getFirstSubSequenceIndex() > 0L)
        {
            projection = (new MatrixInitializer(getModel().getReducedProjection().countRows())
                                  .append(getModel().getReducedProjection().transpose())
                                  .appendRow(new double[]{getModel().getReducedSubSequenceMessage().getSubSequenceX(), getModel().getReducedSubSequenceMessage().getSubSequenceY()})
                                  .create()
                                  .transpose());
        }

        val intersectionCollections = Calculate.intersections(projection, getModel().getFirstSubSequenceIndex(), getModel().getNumberOfIntersectionSegments());
        for (val intersectionCollection : intersectionCollections)
        {
            // TODO: send via DataTransfer?!
            sendIntersections(intersectionCollection);
        }
    }

    private boolean isReadyToCalculateIntersections()
    {
        return getModel().getReducedProjection() != null &&
               getModel().getIntersectionSegmentResponsibilities() != null &&
               (getModel().getFirstSubSequenceIndex() == 0 || getModel().getReducedSubSequenceMessage() != null);
    }

    private void sendIntersections(IntersectionCollection intersectionCollection)
    {
        val responsibleProcessor = responsibleProcessor(intersectionCollection.getIntersectionSegment());

        val intersections = Doubles.toArray(intersectionCollection.getIntersections().stream().map(Intersection::getIntersectionDistance).collect(Collectors.toList()));

        // send intersections directly to the processor which is responsible for the segment
        send(NodeCreationMessages.IntersectionsMessage.builder()
                                                      .sender(getModel().getSelf())
                                                      .receiver(responsibleProcessor)
                                                      .intersectionSegment(intersectionCollection.getIntersectionSegment())
                                                      .intersections(intersections)
                                                      .build());

        // publish intersection event, so that the edge creation protocol does not need to calculate those again
        trySendEvent(ProtocolType.NodeCreation, eventDispatcher -> NodeCreationEvents.IntersectionsCalculatedEvent.builder()
                                                                                                                  .sender(getModel().getSelf())
                                                                                                                  .receiver(eventDispatcher)
                                                                                                                  .intersectionCollection(intersectionCollection)
                                                                                                                  .build());
    }

    private ActorRef responsibleProcessor(int intersectionIndex)
    {
        val processorRootPath = getModel().getIntersectionSegmentResponsibilities().entrySet()
                                          .stream()
                                          .filter(keyValuePair -> keyValuePair.getValue().contains(intersectionIndex))
                                          .map(Map.Entry::getKey)
                                          .findFirst();

        assert processorRootPath.isPresent() : String.format("Unable to find responsible processor for intersection index %1$d!", intersectionIndex);
        return processorRootPath.get();
    }

    private void onIntersections(NodeCreationMessages.IntersectionsMessage message)
    {
        try
        {
            val myResponsibilities = getModel().getIntersectionSegmentResponsibilities().get(getModel().getSelf());
            assert myResponsibilities.contains(message.getIntersectionSegment()) : "Received intersections for an intersection segment we are not responsible for!";

            getModel().getIntersections().putIfAbsent(message.getIntersectionSegment(), new ArrayList<>());
            getModel().getIntersections().get(message.getIntersectionSegment()).add(message.getIntersections());

            // TODO: use round-robin actor pool for better parallelization
            createNodes(message.getIntersectionSegment());
        }
        finally
        {
            complete(message);
        }
    }

    private void createNodes(int intersectionSegment)
    {
        val intersectionList = getModel().getIntersections().get(intersectionSegment);
        if (intersectionList.size() != getModel().getIntersectionSegmentResponsibilities().size())
        {
            // not all processor sent their intersections yet
            return;
        }

        val numberOfIntersections = intersectionList.stream().mapToInt(intersections -> intersections.length).sum();
        val intersections = new double[numberOfIntersections];
        var startIndex = 0;
        for (var i = 0; startIndex < intersections.length; ++i)
        {
            val part = intersectionList.get(i);
            System.arraycopy(part, 0, intersections, startIndex, part.length);
            startIndex += part.length;
        }

        val densitySamples = Calculate.makeRange(0.0d, getModel().getMaximumValue(), NUMBER_OF_DENSITY_SAMPLES);
//        val densitySamples = Calculate.makeRange(0.0d, Doubles.max(intersections) * DENSITY_SAMPLES_SCALING_FACTOR, NUMBER_OF_DENSITY_SAMPLES);

        val h = Calculate.scottsFactor(intersections.length, 1L);
//        val kde = new KernelDensity(intersections, h);
//        val probabilities = new double[NUMBER_OF_DENSITY_SAMPLES];
//
//        for (var i = 0; i < densitySamples.length; ++i)
//        {
//            probabilities[i] = kde.p(densitySamples[i]);
//        }

        val kde = new GaussianKernelDensity(intersections, h);
        val probabilities = kde.evaluate(densitySamples);

        val localMaximumIndices = Calculate.localMaximumIndices(probabilities);
        val nodeCollection = NodeCollection.builder()
                                           .intersectionSegment(intersectionSegment)
                                           .build();
        for (val localMaximumIndex : localMaximumIndices)
        {
            nodeCollection.getNodes().add(Node.builder()
                                              .intersectionLength(densitySamples[localMaximumIndex])
                                              .build());
        }

        publishNodes(nodeCollection);
    }

    private void publishNodes(NodeCollection nodeCollection)
    {
        // TODO: send via DataTransfer

        val nodes = Doubles.toArray(nodeCollection.getNodes().stream().map(Node::getIntersectionLength).collect(Collectors.toList()));
        for (val worker : getModel().getIntersectionSegmentResponsibilities().keySet())
        {
            val protocol = getProtocol(worker.path().root(), ProtocolType.EdgeCreation);
            assert protocol.isPresent() : "Node creation processors must also implement the Edge creation protocol!";

            send(NodeCreationMessages.NodesMessage.builder()
                                                  .sender(getModel().getSelf())
                                                  .receiver(protocol.get().getRootActor())
                                                  .intersectionSegment(nodeCollection.getIntersectionSegment())
                                                  .nodes(nodes)
                                                  .build());
        }
    }
}
