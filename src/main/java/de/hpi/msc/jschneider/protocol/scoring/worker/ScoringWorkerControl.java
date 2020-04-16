package de.hpi.msc.jschneider.protocol.scoring.worker;

import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import de.hpi.msc.jschneider.Debug;
import de.hpi.msc.jschneider.data.graph.GraphEdge;
import de.hpi.msc.jschneider.math.Calculate;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.edgeCreation.EdgeCreationEvents;
import de.hpi.msc.jschneider.protocol.graphMerging.GraphMergingEvents;
import de.hpi.msc.jschneider.protocol.nodeCreation.NodeCreationEvents;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorId;
import de.hpi.msc.jschneider.protocol.scoring.ScoringMessages;
import de.hpi.msc.jschneider.protocol.statistics.StatisticsEvents;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import de.hpi.msc.jschneider.utility.Int64Range;
import de.hpi.msc.jschneider.utility.dataTransfer.DataSource;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleBigArrayBigList;
import it.unimi.dsi.fastutil.doubles.DoubleBigList;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import it.unimi.dsi.fastutil.ints.Int2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntBigArrayBigList;
import it.unimi.dsi.fastutil.ints.IntBigList;
import it.unimi.dsi.fastutil.ints.IntList;
import lombok.val;
import lombok.var;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;

public class ScoringWorkerControl extends AbstractProtocolParticipantControl<ScoringWorkerModel>
{
    public ScoringWorkerControl(ScoringWorkerModel model)
    {
        super(model);
    }

    @Override
    public void preStart()
    {
        super.preStart();

        subscribeToLocalEvent(ProtocolType.NodeCreation, NodeCreationEvents.ResponsibilitiesReceivedEvent.class);
        subscribeToLocalEvent(ProtocolType.EdgeCreation, EdgeCreationEvents.LocalGraphPartitionCreatedEvent.class);
        subscribeToLocalEvent(ProtocolType.GraphMerging, GraphMergingEvents.GraphReceivedEvent.class);
    }

    @Override
    public ImprovedReceiveBuilder complementReceiveBuilder(ImprovedReceiveBuilder builder)
    {
        return super.complementReceiveBuilder(builder)
                    .match(NodeCreationEvents.ResponsibilitiesReceivedEvent.class, this::onResponsibilitiesReceived)
                    .match(EdgeCreationEvents.LocalGraphPartitionCreatedEvent.class, this::onLocalGraphPartitionCreated)
                    .match(GraphMergingEvents.GraphReceivedEvent.class, this::onGraphReceived)
                    .match(ScoringMessages.ScoringParametersMessage.class, this::onScoringParameters)
                    .match(ScoringMessages.MinimumAndMaximumScoreMessage.class, this::onMinimumAndMaximum)
                    .match(ScoringMessages.OverlappingEdgeCreationOrderMessage.class, this::onOverlappingEdgeCreationOrder)
                    .match(ScoringMessages.OverlappingPathScoresMessage.class, this::onOverlappingPathScores);
    }

    private void onResponsibilitiesReceived(NodeCreationEvents.ResponsibilitiesReceivedEvent message)
    {
        try
        {
            assert getModel().getProcessorResponsibleForNextSubSequences() == null : "Received responsibilities already!";

            setProcessorResponsibleForNextSubSequences(message.getSubSequenceResponsibilities());
            getModel().getParticipants().addAll(message.getSubSequenceResponsibilities().keySet());
            getModel().getMissingMinimumAndMaximumValueSenders().addAll(message.getSubSequenceResponsibilities().keySet());
            sendEdgeCreationOrderToNextResponsibleProcessor();
            scoreSubSequences();
        }
        finally
        {
            complete(message);
        }
    }

    private void setProcessorResponsibleForNextSubSequences(Map<ProcessorId, Int64Range> subSequenceResponsibilities)
    {
        val myResponsibilities = subSequenceResponsibilities.get(ProcessorId.of(getModel().getSelf()));
        assert myResponsibilities != null : "We are not responsible for any sub sequences!";

        getModel().setWaitForRemoteEdgeCreationOrder(myResponsibilities.getFrom() > 0L);
        getModel().setWaitForRemotePathScores(myResponsibilities.getFrom() > 0L);

        val nextProcessor = subSequenceResponsibilities.entrySet()
                                                       .stream()
                                                       .filter(entry -> entry.getValue().getFrom() == myResponsibilities.getTo())
                                                       .map(Map.Entry::getKey)
                                                       .findFirst();
        if (!nextProcessor.isPresent())
        {
            return;
        }

        val protocol = getProtocol(nextProcessor.get(), ProtocolType.Scoring);
        assert protocol.isPresent() : "The previous responsible processor does not implement the Scoring protocol!";

        getModel().setProcessorResponsibleForNextSubSequences(protocol.get().getRootActor());
    }

    private void onScoringParameters(ScoringMessages.ScoringParametersMessage message)
    {
        try
        {
            assert getModel().getQueryPathLength() == 0 : "Already received query path length!";

            getModel().setQueryPathLength(message.getQueryPathLength());
            getModel().setSubSequenceLength(message.getSubSequenceLength());
            sendEdgeCreationOrderToNextResponsibleProcessor();
            scoreSubSequences();
        }
        finally
        {
            complete(message);
        }
    }

    private void onLocalGraphPartitionCreated(EdgeCreationEvents.LocalGraphPartitionCreatedEvent message)
    {
        try
        {
            assert getModel().getEdgeCreationOrder() == null : "Edge creation order was received already!";

            val sortedEdgeCreationOrder = message.getGraphPartition().getCreatedEdgesBySubSequenceIndex()
                                                 .long2ObjectEntrySet()
                                                 .stream()
                                                 .sorted(Comparator.comparingLong(Map.Entry::getKey))
                                                 .map(Map.Entry::getValue)
                                                 .collect(Collectors.toList());

            getModel().setEdgeCreationOrder(sortedEdgeCreationOrder);
            sendEdgeCreationOrderToNextResponsibleProcessor();
            scoreSubSequences();
        }
        finally
        {
            complete(message);
        }
    }

    private void sendEdgeCreationOrderToNextResponsibleProcessor()
    {
        if (!isReadyToSendEdgeCreationOrderToPreviousResponsibleProcessor())
        {
            return;
        }

        val overlappingEdgeCreationOrder = new int[getModel().getQueryPathLength() - 1][];
        val edgeCreationOrderOffset = getModel().getEdgeCreationOrder().size() - overlappingEdgeCreationOrder.length;
        for (var edgeCreationOrderIndex = 0; edgeCreationOrderIndex < overlappingEdgeCreationOrder.length; ++edgeCreationOrderIndex)
        {
            overlappingEdgeCreationOrder[edgeCreationOrderIndex] = Ints.toArray(getModel().getEdgeCreationOrder().get(edgeCreationOrderOffset + edgeCreationOrderIndex));
        }

        send(ScoringMessages.OverlappingEdgeCreationOrderMessage.builder()
                                                                .sender(getModel().getSelf())
                                                                .receiver(getModel().getProcessorResponsibleForNextSubSequences())
                                                                .overlappingEdgeCreationOrder(overlappingEdgeCreationOrder)
                                                                .build());
    }

    private boolean isReadyToSendEdgeCreationOrderToPreviousResponsibleProcessor()
    {
        return getModel().getProcessorResponsibleForNextSubSequences() != null
               && getModel().getEdgeCreationOrder() != null
               && !getModel().getEdgeCreationOrder().isEmpty()
               && getModel().getQueryPathLength() > 0;
    }

    private void onOverlappingEdgeCreationOrder(ScoringMessages.OverlappingEdgeCreationOrderMessage message)
    {
        try
        {
            assert getModel().getRemoteEdgeCreationOrder() == null : "Overlapping edge creation order already received!";

            getModel().setRemoteEdgeCreationOrder(message.getOverlappingEdgeCreationOrder());
            scoreSubSequences();
        }
        finally
        {
            complete(message);
        }
    }

    private void onGraphReceived(GraphMergingEvents.GraphReceivedEvent message)
    {
        try
        {
            assert getModel().getEdges() == null : "Graph received already!";

            getModel().setEdges(new Int2ObjectLinkedOpenHashMap<>(message.getGraph()));
            scoreSubSequences();
        }
        finally
        {
            complete(message);
        }
    }

    private void scoreSubSequences()
    {
        if (!isReadyToScoreSubSequences())
        {
            return;
        }

        val startTime = LocalDateTime.now();

        getModel().setNodeDegrees(Calculate.nodeDegrees(getModel().getEdges().values()));
        val combinedEdgeCreationOrder = createEdgeCreationOrder();
//        Debug.print(combinedEdgeCreationOrder, "edge-creation-order.txt");
//        Debug.print(combinedEdgeCreationOrder, String.format("edge-creation-order-%1$s.txt", ProcessorId.of(getModel().getSelf())));

        val pathSummands = new DoubleArrayList(getModel().getQueryPathLength());
        var pathSum = 0.0d;
        var first = true;
        var minScore = Double.MAX_VALUE;
        var maxScore = Double.MIN_VALUE;

        for (var pathStartIndex = 0; pathStartIndex <= combinedEdgeCreationOrder.size() - getModel().getQueryPathLength(); ++pathStartIndex)
        {
            if (first)
            {
                first = false;
                for (var edgeIndex = 0; edgeIndex < getModel().getQueryPathLength(); ++edgeIndex)
                {
                    pathSum = addSummands(pathSummands, combinedEdgeCreationOrder.get(pathStartIndex + edgeIndex), pathSum);
                }
            }
            else
            {
                for (var edgeIndex = 0; edgeIndex < combinedEdgeCreationOrder.get(pathStartIndex - 1).size64(); ++edgeIndex)
                {
                    pathSum -= pathSummands.removeDouble(0);
                }

                pathSum = addSummands(pathSummands, combinedEdgeCreationOrder.get(pathStartIndex + getModel().getQueryPathLength() - 1), pathSum);
            }

            val score = pathSum / pathSummands.size();
            maxScore = Math.max(maxScore, score);
            minScore = Math.min(minScore, score);
            getModel().getPathScores().add(score);
        }

        val endTime = LocalDateTime.now();

        trySendEvent(ProtocolType.Statistics, eventDispatcher -> StatisticsEvents.PathScoresCreatedEvent.builder()
                                                                                                        .sender(getModel().getSelf())
                                                                                                        .receiver(eventDispatcher)
                                                                                                        .startTime(startTime)
                                                                                                        .endTime(endTime)
                                                                                                        .build());

        sendLastPathScoresToNextResponsibleProcessor();
        publishMinimumAndMaximumScore(minScore, maxScore);
    }

    public void publishMinimumAndMaximumScore(double minScore, double maxScore)
    {
        for (val participant : getModel().getParticipants())
        {
            val protocol = getProtocol(participant, ProtocolType.Scoring);
            assert protocol.isPresent() : String.format("The participant (%1$s) does not support the Scoring protocol!", participant);

            send(ScoringMessages.MinimumAndMaximumScoreMessage.builder()
                                                              .sender(getModel().getSelf())
                                                              .receiver(protocol.get().getRootActor())
                                                              .minimumScore(minScore)
                                                              .maximumScore(maxScore)
                                                              .build());
        }
    }

    private void sendLastPathScoresToNextResponsibleProcessor()
    {
        if (getModel().getProcessorResponsibleForNextSubSequences() == null)
        {
            return;
        }

        val scores = getModel().getPathScores().subList(getModel().getPathScores().size() - (getModel().getSubSequenceLength() - 1), getModel().getPathScores().size());
        assert scores.size() == getModel().getSubSequenceLength() - 1 : "Unexpected number of path scores selected!";

        send(ScoringMessages.OverlappingPathScoresMessage.builder()
                                                         .sender(getModel().getSelf())
                                                         .receiver(getModel().getProcessorResponsibleForNextSubSequences())
                                                         .overlappingPathScores(Doubles.toArray(scores))
                                                         .build());
    }

    private void onOverlappingPathScores(ScoringMessages.OverlappingPathScoresMessage message)
    {
        try
        {
            assert getModel().isWaitForRemotePathScores() : "No overlapping path scores expected!";
            assert getModel().getOverlappingPathScores().length == 0 : "Overlapping path scores were received already!";

            getModel().setOverlappingPathScores(message.getOverlappingPathScores());
            calculateRunningMean();
        }
        finally
        {
            complete(message);
        }
    }

    private List<IntBigList> createEdgeCreationOrder()
    {
        if (getModel().getRemoteEdgeCreationOrder() == null)
        {
            return getModel().getEdgeCreationOrder();
        }

        val combinedEdgeCreationOrder = new ArrayList<IntBigList>(getModel().getEdgeCreationOrder());
        val remoteEdgeCreationOrder = new ArrayList<IntBigList>(getModel().getRemoteEdgeCreationOrder().length);
        for (val remoteEdgeCreationOrderPart : getModel().getRemoteEdgeCreationOrder())
        {
            val list = new IntBigArrayBigList(remoteEdgeCreationOrderPart.length);
            for (val entry : remoteEdgeCreationOrderPart)
            {
                list.add(entry);
            }

            remoteEdgeCreationOrder.add(list);
        }

        combinedEdgeCreationOrder.addAll(0, remoteEdgeCreationOrder);

        getModel().setEdgeCreationOrder(null);
        getModel().setRemoteEdgeCreationOrder(null);
        return combinedEdgeCreationOrder;
    }

    private double addSummands(DoubleList pathSummands, IntBigList edgeCreationOrder, double currentPathSum)
    {
        var newPathSum = currentPathSum;
        val edgeCreationOrderIterator = edgeCreationOrder.iterator();
        while (edgeCreationOrderIterator.hasNext())
        {
            val edgeHash = edgeCreationOrderIterator.nextInt();
            val edge = getModel().getEdges().get(edgeHash);
            val nodeDegree = getModel().getNodeDegrees().get(edge.getFrom().hashCode()) - 1L;
            val summand = (double) edge.getWeight() * nodeDegree;
            newPathSum += summand;
            pathSummands.add(summand);
        }

        return newPathSum;
    }

    private boolean isReadyToScoreSubSequences()
    {
        if (getModel().getParticipants().isEmpty())
        {
            return false;
        }

        if (getModel().getQueryPathLength() < 1)
        {
            return false;
        }

        if (getModel().getEdges() == null)
        {
            return false;
        }

        if (getModel().getEdgeCreationOrder() == null || getModel().getEdgeCreationOrder().isEmpty())
        {
            return false;
        }

        if (getModel().isWaitForRemoteEdgeCreationOrder() && getModel().getRemoteEdgeCreationOrder() == null)
        {
            return false;
        }

        return true;
    }

    private void onMinimumAndMaximum(ScoringMessages.MinimumAndMaximumScoreMessage message)
    {
        try
        {
            if (!getModel().getMissingMinimumAndMaximumValueSenders().remove(ProcessorId.of(message.getSender())))
            {
                return;
            }

            getModel().setGlobalMaximumScore(Math.max(getModel().getGlobalMaximumScore(), message.getMaximumScore()));
            getModel().setGlobalMinimumScore(Math.min(getModel().getGlobalMinimumScore(), message.getMinimumScore()));

            calculateRunningMean();
        }
        finally
        {
            complete(message);
        }
    }

    private void calculateRunningMean()
    {
        if (!isReadyToCalculateRunningMean())
        {
            return;
        }

        val startTime = LocalDateTime.now();

        val minScore = getModel().getGlobalMaximumScore() * -1.0d;
        val maxScore = getModel().getGlobalMinimumScore() * -1.0d;
        val scoreRange = maxScore - minScore;
        val normalizationFactor = scoreRange * getModel().getSubSequenceLength();

        val scoresIterator = DoubleStream.concat(Arrays.stream(getModel().getOverlappingPathScores()), getModel().getPathScores().stream().mapToDouble(value -> value))
                                         .map(score -> (-score - minScore) / normalizationFactor)
                                         .iterator();

        val numberOfMeans = (long) getModel().getOverlappingPathScores().length + getModel().getPathScores().size() - getModel().getSubSequenceLength() + 1L;
        var runningMean = 0.0d;
        val runningMeanWindow = new DoubleArrayList(getModel().getSubSequenceLength());
        val runningMeans = new DoubleBigArrayBigList(numberOfMeans);
        var meanIndex = 0L;
        while (meanIndex++ < numberOfMeans)
        {
            if (runningMeans.isEmpty())
            {
                for (var windowIndex = 0; windowIndex < getModel().getSubSequenceLength(); ++windowIndex)
                {
                    val score = scoresIterator.nextDouble();
                    runningMean += score;
                    runningMeanWindow.add(score);
                }
            }
            else
            {
                val score = scoresIterator.nextDouble();
                runningMean -= runningMeanWindow.removeDouble(0);
                runningMean += score;
                runningMeanWindow.add(score);
            }

            runningMeans.add(runningMean);
        }

        val endTime = LocalDateTime.now();

        trySendEvent(ProtocolType.Statistics, eventDispatcher -> StatisticsEvents.PathScoresNormalizedEvent.builder()
                                                                                                           .sender(getModel().getSelf())
                                                                                                           .receiver(eventDispatcher)
                                                                                                           .startTime(startTime)
                                                                                                           .endTime(endTime)
                                                                                                           .build());

        publishPathScores(runningMeans);
    }

    private boolean isReadyToCalculateRunningMean()
    {
        if (!getModel().getMissingMinimumAndMaximumValueSenders().isEmpty())
        {
            return false;
        }

        if (getModel().isWaitForRemotePathScores() && getModel().getOverlappingPathScores().length == 0)
        {
            return false;
        }

        return true;
    }

    private void publishPathScores(DoubleBigList pathScores)
    {
//        Debug.print(pathScores, String.format("path-scores-%1$s.txt", ProcessorId.of(getModel().getSelf())));

        val protocol = getMasterProtocol(ProtocolType.Scoring);
        assert protocol.isPresent() : "The master processor must implement the Scoring protocol!";

        getModel().getDataTransferManager().transfer(DataSource.create(pathScores),
                                                     (dataDistributor, operationId) -> ScoringMessages.InitializePathScoresTransferMessage.builder()
                                                                                                                                          .sender(dataDistributor)
                                                                                                                                          .receiver(protocol.get().getRootActor())
                                                                                                                                          .operationId(operationId)
                                                                                                                                          .build());

        isReadyToBeTerminated();
    }
}
