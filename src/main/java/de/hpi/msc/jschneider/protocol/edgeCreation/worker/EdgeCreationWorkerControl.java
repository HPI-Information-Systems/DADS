package de.hpi.msc.jschneider.protocol.edgeCreation.worker;

import de.hpi.msc.jschneider.data.graph.Graph;
import de.hpi.msc.jschneider.data.graph.GraphNode;
import de.hpi.msc.jschneider.protocol.actorPool.ActorPoolMessages;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.edgeCreation.EdgeCreationEvents;
import de.hpi.msc.jschneider.protocol.edgeCreation.EdgeCreationMessages;
import de.hpi.msc.jschneider.protocol.edgeCreation.worker.graphPartitionCreator.GraphPartitionCreatorMessages;
import de.hpi.msc.jschneider.protocol.edgeCreation.worker.graphPartitionCreator.GraphPartitionCreatorWorkFactory;
import de.hpi.msc.jschneider.protocol.nodeCreation.NodeCreationEvents;
import de.hpi.msc.jschneider.protocol.nodeCreation.NodeCreationMessages;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorId;
import de.hpi.msc.jschneider.protocol.statistics.StatisticsProtocol;
import de.hpi.msc.jschneider.utility.Counter;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import de.hpi.msc.jschneider.utility.dataTransfer.DataReceiver;
import de.hpi.msc.jschneider.utility.dataTransfer.sink.DoublesSink;
import lombok.val;
import lombok.var;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;

public class EdgeCreationWorkerControl extends AbstractProtocolParticipantControl<EdgeCreationWorkerModel>
{
    public EdgeCreationWorkerControl(EdgeCreationWorkerModel model)
    {
        super(model);
    }

    @Override
    public void preStart()
    {
        super.preStart();

        subscribeToLocalEvent(ProtocolType.NodeCreation, NodeCreationEvents.ResponsibilitiesReceivedEvent.class);
        subscribeToLocalEvent(ProtocolType.NodeCreation, NodeCreationEvents.IntersectionsCalculatedEvent.class);
    }

    @Override
    public ImprovedReceiveBuilder complementReceiveBuilder(ImprovedReceiveBuilder builder)
    {
        return super.complementReceiveBuilder(builder)
                    .match(NodeCreationEvents.ResponsibilitiesReceivedEvent.class, this::onResponsibilitiesReceived)
                    .match(NodeCreationEvents.IntersectionsCalculatedEvent.class, this::onIntersectionsCalculated)
                    .match(EdgeCreationMessages.LastNodeMessage.class, this::onLastNode)
                    .match(NodeCreationMessages.InitializeNodesTransferMessage.class, this::acceptNodesTransfer)
                    .match(GraphPartitionCreatorMessages.GraphPartitionCreatedMessage.class, this::onGraphPartitionCreated);
    }

    private void onResponsibilitiesReceived(NodeCreationEvents.ResponsibilitiesReceivedEvent message)
    {
        try
        {
            assert getModel().getLocalSegments() == null : "Already received intersection segment responsibilities!";

            getModel().setLocalSegments(message.getSegmentResponsibilities().get(ProcessorId.of(getModel().getSelf())));
            getModel().setLocalSubSequences(message.getSubSequenceResponsibilities().get(ProcessorId.of(getModel().getSelf())));
            getModel().setNumberOfIntersectionSegments(message.getNumberOfIntersectionSegments());

            setNextResponsibleProcessor(message);

            var nextExpectedSubSequenceIndex = getModel().getLocalSubSequences().getFrom();
            if (nextExpectedSubSequenceIndex > 0L)
            {
                // we must have received a reduced sub sequence from our predecessor processor
                nextExpectedSubSequenceIndex -= 1L;
            }

            getModel().setNextSubSequenceIndex(new Counter(nextExpectedSubSequenceIndex));
            enqueueIntersections();
        }
        finally
        {
            complete(message);
        }
    }

    private void setNextResponsibleProcessor(NodeCreationEvents.ResponsibilitiesReceivedEvent message)
    {
        val nextResponsibleProcessorId = message.getSubSequenceResponsibilities()
                                                .entrySet()
                                                .stream()
                                                .filter(entry -> entry.getValue().getFrom() == getModel().getLocalSubSequences().getTo())
                                                .map(Map.Entry::getKey)
                                                .findFirst();

        if (!nextResponsibleProcessorId.isPresent())
        {
            return;
        }

        val protocol = getProtocol(nextResponsibleProcessorId.get(), ProtocolType.EdgeCreation);
        assert protocol.isPresent() : "The next responsible processor must also implement the edge creation protocol!";

        getLog().debug("Next responsible processor (for edge creation) is {}.", nextResponsibleProcessorId);
        getModel().setNextResponsibleProcessor(protocol.get().getRootActor());
    }

    private void onIntersectionsCalculated(NodeCreationEvents.IntersectionsCalculatedEvent message)
    {
        try
        {
            assert getModel().getIntersectionsInSegment().get(message.getIntersectionCollection().getIntersectionSegment()) == null
                    : "Intersections for this segment have already been calculated!";

            getModel().getIntersectionsInSegment().put(message.getIntersectionCollection().getIntersectionSegment(),
                                                       LocalIntersection.fromIntersectionCollection(message.getIntersectionCollection()));
            enqueueIntersections();
        }
        finally
        {
            complete(message);
        }
    }

    private void enqueueIntersections()
    {
        if (!isReadyToEnqueueIntersections() || getModel().getIntersectionsToMatch() != null)
        {
            return;
        }

        val startTime = LocalDateTime.now();

        val allIntersections = new ArrayList<LocalIntersection>();
        for (val part : getModel().getIntersectionsInSegment().values())
        {
            allIntersections.addAll(part);
        }

        allIntersections.sort(Comparator.comparingLong(LocalIntersection::getCreationIndex));

        val endTime = LocalDateTime.now();

        getModel().setIntersectionsToMatch(allIntersections);

//        Debug.print(getModel().getIntersectionsToMatch().toArray(new LocalIntersection[0]), String.format("%1$s-intersection-creation-order.txt", ProcessorId.of(getModel().getSelf())));

        getLog().info("{} intersections enqueued in {}.",
                      allIntersections.size(),
                      Duration.between(startTime, endTime));

        createGraphPartitions();
    }

    private boolean isReadyToEnqueueIntersections()
    {
        if (getModel().getNumberOfIntersectionSegments() == 0)
        {
            return false;
        }

        if (getModel().getIntersectionsInSegment().size() != getModel().getNumberOfIntersectionSegments())
        {
            return false;
        }

        for (var segment = 0; segment < getModel().getNumberOfIntersectionSegments(); ++segment)
        {
            if (!getModel().getIntersectionsInSegment().containsKey(segment))
            {
                return false;
            }
        }

        return true;
    }

    private void acceptNodesTransfer(NodeCreationMessages.InitializeNodesTransferMessage message)
    {
        assert getModel().getNodesInSegment().get(message.getIntersectionSegment()) == null
                : "Nodes for this segment have already been created!";

        getModel().getDataTransferManager().accept(message,
                                                   dataReceiver ->
                                                   {
                                                       dataReceiver.setState(message.getIntersectionSegment());
                                                       return dataReceiver.addSink(new DoublesSink())
                                                                          .whenFinished(this::onNodesTransferFinished);
                                                   });
    }

    private void onNodesTransferFinished(DataReceiver dataReceiver)
    {
        assert dataReceiver.getState() instanceof Integer : "DataReceiver state should be an Integer!";

        val doublesSink = dataReceiver.getDataSinks().stream().filter(sink -> sink instanceof DoublesSink).findFirst();
        assert doublesSink.isPresent() : "DataReceiver should contain a DoublesSink!";

        val intersectionSegment = (int) dataReceiver.getState();
        val nodes = ((DoublesSink) doublesSink.get()).getDoubles();

        getModel().getNodesInSegment().put(intersectionSegment, nodes);
        trySendLastNodeToNextResponsibleProcessor(intersectionSegment);
        createGraphPartitions();
    }

    private void trySendLastNodeToNextResponsibleProcessor(int intersectionSegment)
    {
        if (getModel().getNextResponsibleProcessor() == null)
        {
            return;
        }

        val lastNode = tryPeekLastNode(intersectionSegment);
        if (!lastNode.isPresent())
        {
            return;
        }

        getLog().debug("Sending last node to {} for edge creation.", ProcessorId.of(getModel().getNextResponsibleProcessor()));

        send(EdgeCreationMessages.LastNodeMessage.builder()
                                                 .sender(getModel().getSelf())
                                                 .receiver(getModel().getNextResponsibleProcessor())
                                                 .lastNode(lastNode.get())
                                                 .build());
    }

    private Optional<GraphNode> tryPeekLastNode(int intersectionSegment)
    {
        if (getModel().getIntersectionsToMatch() == null || getModel().getIntersectionsToMatch().isEmpty())
        {
            return Optional.empty();
        }

        val lastIntersection = getModel().getIntersectionsToMatch().get(getModel().getIntersectionsToMatch().size() - 1);
        if (lastIntersection.getIntersectionSegment() != intersectionSegment)
        {
            return Optional.empty();
        }


        return Optional.of(findClosestNode(lastIntersection));
    }

    private void onLastNode(EdgeCreationMessages.LastNodeMessage message)
    {
        try
        {
            assert getModel().getLastNode() == null : "Already received last node!";

            getLog().debug("Received last node from {}.", ProcessorId.of(message.getSender()));

            getModel().setLastNode(message.getLastNode());
            createGraphPartitions();
        }
        finally
        {
            complete(message);
        }
    }

    private void createGraphPartitions()
    {
        if (!isReadyToCreateGraphPartitions())
        {
            return;
        }

        getModel().setStartTime(LocalDateTime.now());

        val factory = new GraphPartitionCreatorWorkFactory(getModel().getSelf(),
                                                           getModel().getIntersectionsToMatch(),
                                                           getModel().getNodesInSegment(),
                                                           getModel().getNextSubSequenceIndex().get(),
                                                           getModel().getLastNode(),
                                                           getModel().getLocalSubSequences());
        getModel().setExpectedNumberOfGraphPartitions(factory.getNumberOfChunks());
        val actorPool = getLocalProtocol(ProtocolType.ActorPool);
        assert actorPool.isPresent() : "ActorPooling is not supported!";

        send(ActorPoolMessages.ExecuteDistributedFromFactoryMessage.builder()
                                                                   .sender(getModel().getSelf())
                                                                   .receiver(actorPool.get().getRootActor())
                                                                   .workFactory(factory)
                                                                   .build());
    }

    private boolean isReadyToCreateGraphPartitions()
    {
        if (getModel().getIntersectionsToMatch() == null)
        {
            return false;
        }

        if (getModel().getLocalSubSequences().getFrom() > 0L && getModel().getLastNode() == null)
        {
            return false;
        }

        if (getModel().getNodesInSegment().size() != getModel().getNumberOfIntersectionSegments())
        {
            return false;
        }

        return true;
    }

    private void onGraphPartitionCreated(GraphPartitionCreatorMessages.GraphPartitionCreatedMessage message)
    {
        try
        {
            getModel().getGraphPartitions().put(message.getFirstIntersectionCreationIndex(), message.getGraphPartition());
            getLog().info("Received GraphPartition ({} / {}).",
                          getModel().getGraphPartitions().size(),
                          getModel().getExpectedNumberOfGraphPartitions());

            if (getModel().getGraphPartitions().size() != getModel().getExpectedNumberOfGraphPartitions())
            {
                return;
            }

            getModel().setEndTime(LocalDateTime.now());

            val sortedGraphPartitions = getModel().getGraphPartitions().entrySet().stream()
                                                  .sorted(Comparator.comparingLong(Map.Entry::getKey))
                                                  .map(Map.Entry::getValue)
                                                  .toArray(Graph[]::new);

            val graph = new Graph();
            for (val graphPartition : sortedGraphPartitions)
            {
                graph.add(graphPartition);
            }

            if (StatisticsProtocol.IS_ENABLED)
            {
                trySendEvent(ProtocolType.EdgeCreation, eventDispatcher -> EdgeCreationEvents.EdgePartitionCreationCompletedEvent.builder()
                                                                                                                                 .sender(getModel().getSelf())
                                                                                                                                 .receiver(eventDispatcher)
                                                                                                                                 .startTime(getModel().getStartTime())
                                                                                                                                 .endTime(getModel().getEndTime())
                                                                                                                                 .build());
            }

            trySendEvent(ProtocolType.EdgeCreation, eventDispatcher -> EdgeCreationEvents.LocalGraphPartitionCreatedEvent.builder()
                                                                                                                         .sender(getModel().getSelf())
                                                                                                                         .receiver(eventDispatcher)
                                                                                                                         .graphPartition(graph)
                                                                                                                         .build());
        }
        finally
        {
            complete(message);
        }
    }

    private GraphNode findClosestNode(LocalIntersection intersection)
    {
        val nodes = getModel().getNodesInSegment().get(intersection.getIntersectionSegment());
        assert nodes != null : "Nodes must not be null to match an intersection! (That should have been checked earlier)";

        var closestIndex = 0;
        var closestDistance = Double.MAX_VALUE;
        for (var nodeIndex = 0; nodeIndex < nodes.length; ++nodeIndex)
        {
            val distance = Math.abs(nodes[nodeIndex] - intersection.getIntersectionDistance());
//            if (distance < closestDistance)
//            {
//                closestIndex = nodeIndex;
//                closestDistance = distance;
//            }
            if (distance >= closestDistance)
            {
                // since the nodes are already sorted by their distance to the origin,
                // we can safely assume that once the distance is getting bigger we
                // have found the best match
                break;
            }

            closestIndex = nodeIndex;
            closestDistance = distance;
        }

        return GraphNode.builder()
                        .intersectionSegment(intersection.getIntersectionSegment())
                        .index(closestIndex)
                        .build();
    }
}
