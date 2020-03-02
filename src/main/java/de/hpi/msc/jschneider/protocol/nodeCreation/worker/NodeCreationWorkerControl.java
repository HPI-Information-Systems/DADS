package de.hpi.msc.jschneider.protocol.nodeCreation.worker;

import akka.actor.ActorRef;
import com.google.common.primitives.Doubles;
import de.hpi.msc.jschneider.math.Calculate;
import de.hpi.msc.jschneider.math.Intersection;
import de.hpi.msc.jschneider.math.IntersectionCollection;
import de.hpi.msc.jschneider.protocol.actorPool.ActorPoolMessages;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.dimensionReduction.DimensionReductionEvents;
import de.hpi.msc.jschneider.protocol.nodeCreation.NodeCreationEvents;
import de.hpi.msc.jschneider.protocol.nodeCreation.NodeCreationMessages;
import de.hpi.msc.jschneider.protocol.nodeCreation.worker.intersectionCalculator.IntersectionCalculatorMessages;
import de.hpi.msc.jschneider.protocol.nodeCreation.worker.intersectionCalculator.IntersectionWorkFactory;
import de.hpi.msc.jschneider.protocol.nodeCreation.worker.nodeExtractor.NodeExtractor;
import de.hpi.msc.jschneider.protocol.nodeCreation.worker.nodeExtractor.NodeExtractorMessages;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorId;
import de.hpi.msc.jschneider.protocol.statistics.StatisticsProtocol;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import de.hpi.msc.jschneider.utility.Int64Range;
import de.hpi.msc.jschneider.utility.MatrixInitializer;
import de.hpi.msc.jschneider.utility.dataTransfer.DataReceiver;
import de.hpi.msc.jschneider.utility.dataTransfer.sink.DoublesSink;
import de.hpi.msc.jschneider.utility.dataTransfer.source.GenericDataSource;
import lombok.val;
import lombok.var;
import org.ojalgo.function.aggregator.Aggregator;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;
import java.util.stream.Collectors;

public class NodeCreationWorkerControl extends AbstractProtocolParticipantControl<NodeCreationWorkerModel>
{
    private static final int NUMBER_OF_DENSITY_SAMPLES = 250;

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
                    .match(NodeCreationMessages.InitializeIntersectionsTransferMessage.class, this::acceptIntersectionsTransfer)
                    .match(NodeExtractorMessages.NodeCollectionCreatedMessage.class, this::onNodeCollectionCreated)
                    .match(IntersectionCalculatorMessages.IntersectionsCalculatedMessage.class, this::onIntersectionsCalculated);
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
            getModel().setDensitySamples(Calculate.makeRange(0.0d, getModel().getMaximumValue(), NUMBER_OF_DENSITY_SAMPLES));

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

        getLog().error("Unable to find responsible actor for subsequence index {}!", subSequenceIndexToSend);
    }

    private void onReducedSubSequence(NodeCreationMessages.ReducedSubSequenceMessage message)
    {
        try
        {
            assert getModel().getReducedSubSequenceMessage() == null : "Already received a ReducedSubSequence message!";
            assert getModel().getFirstSubSequenceIndex() == message.getSubSequenceIndex() + 1 : "Unexpected reduced sub sequence index!";

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

        getModel().setStartTime(LocalDateTime.now());

        var projection = getModel().getReducedProjection();
        var firstSubSequenceIndex = getModel().getFirstSubSequenceIndex();
        if (getModel().getFirstSubSequenceIndex() > 0L)
        {
            firstSubSequenceIndex -= 1;
            projection = (new MatrixInitializer(getModel().getReducedProjection().countRows())
                                  .appendRow(new double[]{getModel().getReducedSubSequenceMessage().getSubSequenceX(), getModel().getReducedSubSequenceMessage().getSubSequenceY()})
                                  .append(getModel().getReducedProjection().transpose())
                                  .create()
                                  .transpose());
        }

        val actorPool = getLocalProtocol(ProtocolType.ActorPool);
        assert actorPool.isPresent() : "ActorPooling is not supported!";

        val workFactory = new IntersectionWorkFactory(getModel().getSelf(), projection, getModel().getNumberOfIntersectionSegments(), firstSubSequenceIndex);
        getModel().setExpectedNumberOfIntersectionCollections(workFactory.getNumberOfIntersectionChunks());

        send(ActorPoolMessages.ExecuteDistributedFromFactoryMessage.builder()
                                                                   .sender(getModel().getSelf())
                                                                   .receiver(actorPool.get().getRootActor())
                                                                   .workFactory(workFactory)
                                                                   .build());
    }

    private boolean isReadyToCalculateIntersections()
    {
        if (getModel().getReducedProjection() == null)
        {
//            getLog().info("Unable to start calculating intersections: ReducedProjection == null.");
            return false;
        }

        if (getModel().getIntersectionSegmentResponsibilities() == null)
        {
//            getLog().info("Unable to start calculating intersections: IntersectionSegmentResponsibilities == null.");
            return false;
        }

        if (getModel().getFirstSubSequenceIndex() > 0L && getModel().getReducedSubSequenceMessage() == null)
        {
//            getLog().info("Unable to start calculating intersections: ReducedSubSequenceMessage == null.");
            return false;
        }

//        getLog().info("Ready to start calculating intersections!");
        return true;
    }

    private void onIntersectionsCalculated(IntersectionCalculatorMessages.IntersectionsCalculatedMessage message)
    {
        try
        {
            getModel().getIntersectionCollections().add(message.getIntersectionCollections());

            getLog().info("Received IntersectionCollections ({} / {}).",
                          getModel().getIntersectionCollections().size(),
                          getModel().getExpectedNumberOfIntersectionCollections());

            if (getModel().getIntersectionCollections().size() < getModel().getExpectedNumberOfIntersectionCollections())
            {
                return;
            }

            val intersectionCollections = combineIntersectionCollections();
//            Debug.print(intersectionCollections, String.format("%1$s-intersection-collections.txt", ProcessorId.of(getModel().getSelf())));

            for (val intersectionCollection : intersectionCollections)
            {
                sendIntersections(intersectionCollection);
            }
        }
        finally
        {
            complete(message);
        }
    }

    private IntersectionCollection[] combineIntersectionCollections()
    {
        val result = new IntersectionCollection[getModel().getNumberOfIntersectionSegments()];
        for (var intersectionSegment = 0; intersectionSegment < result.length; ++intersectionSegment)
        {
            result[intersectionSegment] = IntersectionCollection.builder()
                                                                .intersectionSegment(intersectionSegment)
                                                                .build();
        }

        for (var collections : getModel().getIntersectionCollections())
        {
            for (var collection : collections)
            {
                result[collection.getIntersectionSegment()].getIntersections().addAll(collection.getIntersections());
            }
        }

        return result;
    }

    private void sendIntersections(IntersectionCollection intersectionCollection)
    {
        val responsibleProcessor = responsibleProcessor(intersectionCollection.getIntersectionSegment());

        val intersections = Doubles.toArray(intersectionCollection.getIntersections()
                                                                  .stream()
                                                                  .sorted(Comparator.comparingLong(Intersection::getCreationIndex))
                                                                  .map(Intersection::getIntersectionDistance).collect(Collectors.toList()));

        getModel().getDataTransferManager().transfer(GenericDataSource.create(intersections),
                                                     (dataDistributor, operationId) -> NodeCreationMessages.InitializeIntersectionsTransferMessage.builder()
                                                                                                                                                  .sender(dataDistributor)
                                                                                                                                                  .receiver(responsibleProcessor)
                                                                                                                                                  .operationId(operationId)
                                                                                                                                                  .intersectionSegment(intersectionCollection.getIntersectionSegment())
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

    private void acceptIntersectionsTransfer(NodeCreationMessages.InitializeIntersectionsTransferMessage message)
    {
        val myResponsibilities = getModel().getIntersectionSegmentResponsibilities().get(getModel().getSelf());
        assert myResponsibilities.contains(message.getIntersectionSegment()) : "Received intersections for an intersection segment we are not responsible for!";

        getModel().getIntersections().putIfAbsent(message.getIntersectionSegment(), new ArrayList<>());
        getModel().getDataTransferManager().accept(message,
                                                   dataReceiver ->
                                                   {
                                                       dataReceiver.setState(message.getIntersectionSegment());
                                                       return dataReceiver.addSink(new DoublesSink())
                                                                          .whenFinished(this::onIntersectionsTransferFinished);
                                                   });
    }

    private void onIntersectionsTransferFinished(DataReceiver dataReceiver)
    {
        assert dataReceiver.getState() instanceof Integer : "DataReceiver state should be an Integer!";

        val doublesSink = dataReceiver.getDataSinks().stream().filter(sink -> sink instanceof DoublesSink).findFirst();
        assert doublesSink.isPresent() : "DataReceiver should contain a DoublesSink!";

        val intersectionSegment = (int) dataReceiver.getState();
        val intersections = ((DoublesSink) doublesSink.get()).getDoubles();
        getModel().getIntersections().get(intersectionSegment).add(intersections);

        createNodes(intersectionSegment);
    }

    private void createNodes(int intersectionSegment)
    {
        val intersectionList = getModel().getIntersections().get(intersectionSegment);
        if (intersectionList.size() != getModel().getIntersectionSegmentResponsibilities().size())
        {
            // not all processor sent their intersections yet
            return;
        }

        val actorPool = getLocalProtocol(ProtocolType.ActorPool);
        assert actorPool.isPresent() : "ActorPooling is not supported!";

        send(NodeExtractorMessages.CreateNodeCollectionMessage.builder()
                                                              .sender(getModel().getSelf())
                                                              .receiver(actorPool.get().getRootActor())
                                                              .consumer(new NodeExtractor())
                                                              .intersectionSegment(intersectionSegment)
                                                              .intersections(getModel().getIntersections().get(intersectionSegment))
                                                              .densitySamples(getModel().getDensitySamples())
                                                              .participants(getModel().getIntersectionSegmentResponsibilities().keySet().stream().map(ProcessorId::of).collect(Collectors.toSet()))
                                                              .build());
    }

    private void onNodeCollectionCreated(NodeExtractorMessages.NodeCollectionCreatedMessage message)
    {
        try
        {
            getModel().getNodeCollections().put(message.getNodeCollection().getIntersectionSegment(), message.getNodeCollection());

            if (getModel().getNodeCollections().size() != getModel().getNumberOfIntersectionSegments())
            {
                return;
            }

            getModel().setEndTime(LocalDateTime.now());

            if (StatisticsProtocol.IS_ENABLED)
            {
                trySendEvent(ProtocolType.NodeCreation, eventDispatcher -> NodeCreationEvents.NodePartitionCreationCompletedEvent.builder()
                                                                                                                                 .sender(getModel().getSelf())
                                                                                                                                 .receiver(eventDispatcher)
                                                                                                                                 .startTime(getModel().getStartTime())
                                                                                                                                 .endTime(getModel().getEndTime())
                                                                                                                                 .build());
            }

//            Debug.print(getModel().getNodeCollections().values().toArray(new NodeCollection[0]), String.format("%1$s-nodes.txt", ProcessorId.of(getModel().getSelf())));
        }
        finally
        {
            complete(message);
        }
    }
}
