package de.hpi.msc.jschneider.protocol.nodeCreation.worker;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.math.Calculate;
import de.hpi.msc.jschneider.math.IntersectionCollection;
import de.hpi.msc.jschneider.protocol.actorPool.ActorPoolMessages;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.dimensionReduction.DimensionReductionEvents;
import de.hpi.msc.jschneider.protocol.nodeCreation.NodeCreationEvents;
import de.hpi.msc.jschneider.protocol.nodeCreation.NodeCreationMessages;
import de.hpi.msc.jschneider.protocol.nodeCreation.densityEstimator.DensityEstimatorControl;
import de.hpi.msc.jschneider.protocol.nodeCreation.densityEstimator.DensityEstimatorModel;
import de.hpi.msc.jschneider.protocol.nodeCreation.densityEstimator.calculator.DensityCalculatorMessages;
import de.hpi.msc.jschneider.protocol.nodeCreation.worker.intersectionCalculator.IntersectionCalculatorMessages;
import de.hpi.msc.jschneider.protocol.nodeCreation.worker.intersectionCalculator.IntersectionWorkFactory;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorId;
import de.hpi.msc.jschneider.protocol.statistics.StatisticsEvents;
import de.hpi.msc.jschneider.utility.Counter;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import de.hpi.msc.jschneider.utility.Int64Range;
import de.hpi.msc.jschneider.utility.dataTransfer.DataSource;
import de.hpi.msc.jschneider.utility.dataTransfer.sink.DoubleSink;
import de.hpi.msc.jschneider.utility.matrix.RowMatrixBuilder;
import it.unimi.dsi.fastutil.doubles.DoubleBigArrayBigList;
import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import lombok.val;
import lombok.var;
import org.ojalgo.function.aggregator.Aggregator;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public class NodeCreationWorkerControl extends AbstractProtocolParticipantControl<NodeCreationWorkerModel>
{
    private static final long NUMBER_OF_DENSITY_SAMPLES = 250;

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
                    .match(DensityCalculatorMessages.NodeCollectionCreatedMessage.class, this::onNodeCollectionCreated)
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
            getLog().info("Received intersection segment responsibilities: [{}, {})",
                          message.getIntersectionSegmentResponsibilities().get(getModel().getSelf()).getFrom(),
                          message.getIntersectionSegmentResponsibilities().get(getModel().getSelf()).getTo());

            getModel().setIntersectionSegmentResponsibilities(message.getIntersectionSegmentResponsibilities());
            getModel().setSubSequenceResponsibilities(message.getSubSequenceResponsibilities());
            getModel().setMaximumValue(message.getMaximumValue());
            getModel().setNumberOfIntersectionSegments(message.getNumberOfIntersectionSegments());
            getModel().setDensitySamples(Calculate.makeRange(0.0d, getModel().getMaximumValue(), NUMBER_OF_DENSITY_SAMPLES));

            getModel().setIntersections(new Int2ObjectArrayMap<>(getModel().getIntersectionSegmentResponsibilities().get(getModel().getSelf()).length()));
            getModel().setNumberOfReceivedIntersectionCollections(new Int2ObjectArrayMap<>(getModel().getIntersectionSegmentResponsibilities().get(getModel().getSelf()).length()));

            val transformedSubSequenceResponsibilities = message.getSubSequenceResponsibilities().entrySet().stream().collect(Collectors.toMap(e -> ProcessorId.of(e.getKey()), Map.Entry::getValue));
            val transformedSegmentResponsibilities = message.getIntersectionSegmentResponsibilities().entrySet().stream().collect(Collectors.toMap(e -> ProcessorId.of(e.getKey()), Map.Entry::getValue));

            getModel().setParticipants(transformedSubSequenceResponsibilities.keySet());
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

        val nextSubSequenceIndex = getModel().getFirstSubSequenceIndex() + getModel().getReducedProjection().countColumns();
        for (val entry : getModel().getSubSequenceResponsibilities().entrySet())
        {
            val subSequenceIndices = entry.getValue();
            if (!subSequenceIndices.contains(nextSubSequenceIndex))
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
                                                               .subSequenceIndex(nextSubSequenceIndex - 1)
                                                               .subSequenceX(x)
                                                               .subSequenceY(y)
                                                               .build());
            return;
        }

        getLog().error("Unable to find responsible actor for subsequence index {}!", nextSubSequenceIndex);
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

        getModel().setIntersectionCalculationStartTime(LocalDateTime.now());

        var projection = getModel().getReducedProjection();
        var firstSubSequenceIndex = getModel().getFirstSubSequenceIndex();
        if (getModel().getFirstSubSequenceIndex() > 0L)
        {
            firstSubSequenceIndex -= 1;
            projection = (new RowMatrixBuilder(getModel().getReducedProjection().countRows())
                                  .append(new double[]{getModel().getReducedSubSequenceMessage().getSubSequenceX(), getModel().getReducedSubSequenceMessage().getSubSequenceY()})
                                  .append(getModel().getReducedProjection().transpose())
                                  .build()
                                  .transpose());
        }

        val actorPool = getLocalProtocol(ProtocolType.ActorPool);
        assert actorPool.isPresent() : "ActorPooling is not supported!";

        val workFactory = new IntersectionWorkFactory(getModel().getSelf(), projection, getModel().getNumberOfIntersectionSegments(), firstSubSequenceIndex);
        getModel().setExpectedNumberOfIntersectionCollections(workFactory.getNumberOfIntersectionChunks());
        getModel().setIntersectionCollections(new ArrayList<>(getModel().getExpectedNumberOfIntersectionCollections()));

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
            return false;
        }

        if (getModel().getIntersectionSegmentResponsibilities() == null)
        {
            return false;
        }

        if (getModel().getFirstSubSequenceIndex() > 0L && getModel().getReducedSubSequenceMessage() == null)
        {
            return false;
        }

        return true;
    }

    private void onIntersectionsCalculated(IntersectionCalculatorMessages.IntersectionsCalculatedMessage message)
    {
        try
        {
            getModel().getIntersectionCollections().add(message.getIntersectionCollections());
            getModel().getTotalNumberOfIntersections().increment(Arrays.stream(message.getIntersectionCollections()).mapToLong(collection -> collection.getIntersections().size64()).sum());

            getLog().info("Received IntersectionCollections ({} / {}).",
                          getModel().getIntersectionCollections().size(),
                          getModel().getExpectedNumberOfIntersectionCollections());

            if (getModel().getIntersectionCollections().size() < getModel().getExpectedNumberOfIntersectionCollections())
            {
                return;
            }

            val intersectionCollections = combineIntersectionCollections();

//            Debug.print(intersectionCollections, "intersections.txt");

            getModel().setIntersectionCalculationEndTime(LocalDateTime.now());

            trySendEvent(ProtocolType.Statistics, eventDispatcher -> StatisticsEvents.IntersectionsCreatedEvent.builder()
                                                                                                               .sender(getModel().getSelf())
                                                                                                               .receiver(eventDispatcher)
                                                                                                               .startTime(getModel().getIntersectionCalculationStartTime())
                                                                                                               .endTime(getModel().getIntersectionCalculationEndTime())
                                                                                                               .build());

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
        val averageNumberOfIntersections = (long) Math.ceil(getModel().getTotalNumberOfIntersections().get() / (double) getModel().getNumberOfIntersectionSegments());
        for (var intersectionSegment = 0; intersectionSegment < result.length; ++intersectionSegment)
        {
            result[intersectionSegment] = new IntersectionCollection(intersectionSegment, averageNumberOfIntersections);
        }

        for (var collections : getModel().getIntersectionCollections())
        {
            for (var collection : collections)
            {
                result[collection.getIntersectionSegment()].getIntersections().addAll(collection.getIntersections());
            }
        }
        getModel().getIntersectionCollections().clear();

        return result;
    }

    private void sendIntersections(IntersectionCollection intersectionCollection)
    {
        val responsibleProcessor = responsibleProcessor(intersectionCollection.getIntersectionSegment());

        getModel().getDataTransferManager().transfer(DataSource.create(intersectionCollection),
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

        val estimatedNumberOfIntersections = getModel().getReducedProjection().countColumns() * getModel().getSubSequenceResponsibilities().size() / (double) getModel().getNumberOfIntersectionSegments();

        getModel().getIntersections().putIfAbsent(message.getIntersectionSegment(), new DoubleBigArrayBigList((long) estimatedNumberOfIntersections));
        getModel().getNumberOfReceivedIntersectionCollections().putIfAbsent(message.getIntersectionSegment(), new Counter(0L));
        getModel().getDataTransferManager().accept(message,
                                                   dataReceiver ->
                                                   {
                                                       val sink = new DoubleSink();
                                                       return dataReceiver.addSink(sink)
                                                                          .whenFinished(receiver -> onIntersectionsTransferFinished(message.getIntersectionSegment(), sink));
                                                   });
    }

    private void onIntersectionsTransferFinished(int intersectionSegment, DoubleSink sink)
    {
        getModel().getIntersections().get(intersectionSegment).addAll(sink.getDoubles());
        getModel().getNumberOfReceivedIntersectionCollections().get(intersectionSegment).increment();
        createNodes(intersectionSegment);
    }

    private void createNodes(int intersectionSegment)
    {
        if (getModel().getNumberOfReceivedIntersectionCollections().get(intersectionSegment).get() != getModel().getIntersectionSegmentResponsibilities().size())
        {
            // not all processor sent their intersections yet
            return;
        }

        if (getModel().getNodeExtractionStartTime() == null)
        {
            getModel().setNodeExtractionStartTime(LocalDateTime.now());
        }

        val model = DensityEstimatorModel.builder()
                                         .supervisor(getModel().getSelf())
                                         .intersectionSegment(intersectionSegment)
                                         .participants(getModel().getParticipants())
                                         .samples(getModel().getIntersections().get(intersectionSegment))
                                         .pointsToEvaluate(getModel().getDensitySamples())
                                         .build();
        val control = new DensityEstimatorControl(model);
        val estimator = trySpawnChild(control, "DensityEstimator");
        if (!estimator.isPresent())
        {
            getLog().error("Unable to create new DensityEstimator!");
        }
    }

    private void onNodeCollectionCreated(DensityCalculatorMessages.NodeCollectionCreatedMessage message)
    {
        try
        {
            getModel().getNodeCollections().put(message.getNodeCollection().getIntersectionSegment(), message.getNodeCollection());

            getLog().info("Received NodeCollection ({} / {}).",
                          getModel().getNodeCollections().size(),
                          getModel().getIntersectionSegmentResponsibilities().get(getModel().getSelf()).length());

            if (getModel().getNodeCollections().size() != getModel().getIntersectionSegmentResponsibilities().get(getModel().getSelf()).length())
            {
                return;
            }

//            Debug.print(getModel().getNodeCollections().values().toArray(new NodeCollection[0]), String.format("%1$s-nodes.txt", ProcessorId.of(getModel().getSelf())));

            getModel().setNodeExtractionEndTime(LocalDateTime.now());

            trySendEvent(ProtocolType.Statistics, eventDispatcher -> StatisticsEvents.NodesExtractedEvent.builder()
                                                                                                         .sender(getModel().getSelf())
                                                                                                         .receiver(eventDispatcher)
                                                                                                         .startTime(getModel().getNodeExtractionStartTime())
                                                                                                         .endTime(getModel().getNodeExtractionEndTime())
                                                                                                         .build());

//            Debug.print(getModel().getNodeCollections().values().toArray(new NodeCollection[0]), String.format("%1$s-nodes.txt", ProcessorId.of(getModel().getSelf())));

            isReadyToBeTerminated();
        }
        finally
        {
            complete(message);
        }
    }
}
