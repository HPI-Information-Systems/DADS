package de.hpi.msc.jschneider.protocol.scoring.worker;

import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import de.hpi.msc.jschneider.math.Calculate;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.edgeCreation.EdgeCreationEvents;
import de.hpi.msc.jschneider.protocol.graphMerging.GraphMergingEvents;
import de.hpi.msc.jschneider.protocol.nodeCreation.NodeCreationEvents;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorId;
import de.hpi.msc.jschneider.protocol.scoring.ScoringEvents;
import de.hpi.msc.jschneider.protocol.scoring.ScoringMessages;
import de.hpi.msc.jschneider.protocol.statistics.StatisticsProtocol;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import de.hpi.msc.jschneider.utility.Int64Range;
import de.hpi.msc.jschneider.utility.dataTransfer.source.GenericDataSource;
import lombok.val;
import lombok.var;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
                                                 .entrySet()
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

            getModel().setEdges(message.getGraph());
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
//        Debug.print(combinedEdgeCreationOrder, String.format("edge-creation-order-%1$s.txt", ProcessorId.of(getModel().getSelf())));

        val pathSummands = new ArrayList<Double>(getModel().getQueryPathLength());
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
                for (var edgeIndex = 0; edgeIndex < combinedEdgeCreationOrder.get(pathStartIndex - 1).size(); ++edgeIndex)
                {
                    pathSum -= pathSummands.remove(0);
                }

                pathSum = addSummands(pathSummands, combinedEdgeCreationOrder.get(pathStartIndex + getModel().getQueryPathLength() - 1), pathSum);
            }

            val score = pathSum / pathSummands.size();
            maxScore = Math.max(maxScore, score);
            minScore = Math.min(minScore, score);
            getModel().getPathScores().add(score);
        }

        val endTime = LocalDateTime.now();

        if (StatisticsProtocol.IS_ENABLED)
        {
            trySendEvent(ProtocolType.Scoring, eventDispatcher -> ScoringEvents.PathScoringCompletedEvent.builder()
                                                                                                         .sender(getModel().getSelf())
                                                                                                         .receiver(eventDispatcher)
                                                                                                         .startTime(startTime)
                                                                                                         .endTime(endTime)
                                                                                                         .build());
        }

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

    private List<List<Integer>> createEdgeCreationOrder()
    {
        if (getModel().getRemoteEdgeCreationOrder() == null)
        {
            return getModel().getEdgeCreationOrder();
        }

        val combinedEdgeCreationOrder = new ArrayList<List<Integer>>(getModel().getEdgeCreationOrder());
        val remoteEdgeCreationOrder = new ArrayList<List<Integer>>();
        for (val remoteEdgeCreationOrderPart : getModel().getRemoteEdgeCreationOrder())
        {
            remoteEdgeCreationOrder.add(Arrays.stream(remoteEdgeCreationOrderPart).boxed().collect(Collectors.toList()));
        }

        combinedEdgeCreationOrder.addAll(0, remoteEdgeCreationOrder);

        return combinedEdgeCreationOrder;
    }

    private double addSummands(List<Double> pathSummands, List<Integer> edgeCreationOrder, double currentPathSum)
    {
        var newPathSum = currentPathSum;
        for (val edgeHash : edgeCreationOrder)
        {
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
        val normalizedScores = new double[getModel().getPathScores().size() + getModel().getOverlappingPathScores().length];
        for (var scoreIndex = 0; scoreIndex < normalizedScores.length; ++scoreIndex)
        {
            var score = 0.0d;
            if (scoreIndex < getModel().getOverlappingPathScores().length)
            {
                score = getModel().getOverlappingPathScores()[scoreIndex];
            }
            else
            {
                score = getModel().getPathScores().get(scoreIndex - getModel().getOverlappingPathScores().length);
            }

            normalizedScores[scoreIndex] = (-score - minScore) / scoreRange;
        }

        var runningMean = 0.0d;
        val runningMeans = new ArrayList<Double>();
        final double windowSizeAsDouble = getModel().getSubSequenceLength();
        for (var runningMeanStartIndex = 0; runningMeanStartIndex <= normalizedScores.length - getModel().getSubSequenceLength(); ++runningMeanStartIndex)
        {
            if (runningMeans.isEmpty())
            {
                for (var windowIndex = 0; windowIndex < getModel().getSubSequenceLength(); ++windowIndex)
                {
                    runningMean += normalizedScores[runningMeanStartIndex + windowIndex] / windowSizeAsDouble;
                }
            }
            else
            {
                runningMean -= normalizedScores[runningMeanStartIndex - 1] / windowSizeAsDouble;
                runningMean += normalizedScores[runningMeanStartIndex + getModel().getSubSequenceLength() - 1] / windowSizeAsDouble;
            }

            runningMeans.add(runningMean);
        }

        val endTime = LocalDateTime.now();

        if (StatisticsProtocol.IS_ENABLED)
        {
            trySendEvent(ProtocolType.Scoring, eventDispatcher -> ScoringEvents.PathScoreNormalizationCompletedEvent.builder()
                                                                                                                    .sender(getModel().getSelf())
                                                                                                                    .receiver(eventDispatcher)
                                                                                                                    .startTime(startTime)
                                                                                                                    .endTime(endTime)
                                                                                                                    .build());
        }

        publishPathScores(Doubles.toArray(runningMeans));
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

    private void publishPathScores(double[] pathScores)
    {
//        Debug.print(pathScores, String.format("path-scores-%1$s.txt", ProcessorId.of(getModel().getSelf())));

        val protocol = getMasterProtocol(ProtocolType.Scoring);
        assert protocol.isPresent() : "The master processor must implement the Scoring protocol!";

        getModel().getDataTransferManager().transfer(GenericDataSource.create(pathScores),
                                                     (dataDistributor, operationId) -> ScoringMessages.InitializePathScoresTransferMessage.builder()
                                                                                                                                          .sender(dataDistributor)
                                                                                                                                          .receiver(protocol.get().getRootActor())
                                                                                                                                          .operationId(operationId)
                                                                                                                                          .build());
    }
}
