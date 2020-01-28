package de.hpi.msc.jschneider.protocol.scoring.worker;

import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;
import de.hpi.msc.jschneider.math.Calculate;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.edgeCreation.EdgeCreationEvents;
import de.hpi.msc.jschneider.protocol.graphMerging.GraphMergingEvents;
import de.hpi.msc.jschneider.protocol.nodeCreation.NodeCreationEvents;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorId;
import de.hpi.msc.jschneider.protocol.scoring.ScoringMessages;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import de.hpi.msc.jschneider.utility.Int64Range;
import de.hpi.msc.jschneider.utility.dataTransfer.source.GenericDataSource;
import lombok.val;
import lombok.var;

import java.util.ArrayList;
import java.util.Arrays;
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
                    .match(ScoringMessages.QueryPathLengthMessage.class, this::onQueryPathLength)
                    .match(ScoringMessages.OverlappingEdgeCreationOrder.class, this::onOverlappingEdgeCreationOrder);
    }

    private void onResponsibilitiesReceived(NodeCreationEvents.ResponsibilitiesReceivedEvent message)
    {
        try
        {
            assert getModel().getProcessorResponsibleForPreviousSubSequences() == null : "Received responsibilities already!";

            getModel().setResponsibilitiesReceived(true);
            setProcessorResponsibleForPreviousSubSequences(message.getSubSequenceResponsibilities());
            sendEdgeCreationOrderToPreviousResponsibleProcessor();
            scoreSubSequences();
        }
        finally
        {
            complete(message);
        }
    }

    private void setProcessorResponsibleForPreviousSubSequences(Map<ProcessorId, Int64Range> subSequenceResponsibilities)
    {
        val myResponsibilities = subSequenceResponsibilities.get(ProcessorId.of(getModel().getSelf()));
        assert myResponsibilities != null : "We are not responsible for any sub sequences!";

        getModel().setWaitForRemoteEdgeCreationOrder(subSequenceResponsibilities.values().stream().anyMatch(entry -> entry.getFrom() == myResponsibilities.getTo()));

        val previousProcessor = subSequenceResponsibilities.entrySet().stream().filter(entry -> entry.getValue().getTo() == myResponsibilities.getFrom()).map(Map.Entry::getKey).findFirst();
        if (!previousProcessor.isPresent())
        {
            return;
        }

        val protocol = getProtocol(previousProcessor.get(), ProtocolType.Scoring);
        assert protocol.isPresent() : "The previous responsible processor does not implement the Scoring protocol!";

        getModel().setProcessorResponsibleForPreviousSubSequences(protocol.get().getRootActor());
    }

    private void onQueryPathLength(ScoringMessages.QueryPathLengthMessage message)
    {
        try
        {
            assert getModel().getQueryPathLength() == 0 : "Already received query path length!";

            getModel().setQueryPathLength(message.getQueryPathLength());
            sendEdgeCreationOrderToPreviousResponsibleProcessor();
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

            val sortedEdgeCreationOrder = message.getGraphPartition().getCreatedEdgesBySubSequenceIndex().entrySet().stream()
                                                 .sorted((a, b) -> (int) (a.getKey() - b.getKey()))
                                                 .map(Map.Entry::getValue)
                                                 .collect(Collectors.toList());

            getModel().setEdgeCreationOrder(sortedEdgeCreationOrder);
            sendEdgeCreationOrderToPreviousResponsibleProcessor();
            scoreSubSequences();
        }
        finally
        {
            complete(message);
        }
    }

    private void sendEdgeCreationOrderToPreviousResponsibleProcessor()
    {
        if (!isReadyToSendEdgeCreationOrderToPreviousResponsibleProcessor())
        {
            return;
        }

        val overlappingEdgeCreationOrder = new int[getModel().getQueryPathLength() - 1][];
        for (var edgeCreationOrderIndex = 0; edgeCreationOrderIndex < overlappingEdgeCreationOrder.length; ++edgeCreationOrderIndex)
        {
            overlappingEdgeCreationOrder[edgeCreationOrderIndex] = Ints.toArray(getModel().getEdgeCreationOrder().get(edgeCreationOrderIndex));
        }

        send(ScoringMessages.OverlappingEdgeCreationOrder.builder()
                                                         .sender(getModel().getSelf())
                                                         .receiver(getModel().getProcessorResponsibleForPreviousSubSequences())
                                                         .overlappingEdgeCreationOrder(overlappingEdgeCreationOrder)
                                                         .build());
    }

    private boolean isReadyToSendEdgeCreationOrderToPreviousResponsibleProcessor()
    {
        return getModel().getProcessorResponsibleForPreviousSubSequences() != null
               && getModel().getEdgeCreationOrder() != null
               && !getModel().getEdgeCreationOrder().isEmpty()
               && getModel().getQueryPathLength() > 0;
    }

    private void onOverlappingEdgeCreationOrder(ScoringMessages.OverlappingEdgeCreationOrder message)
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

        getModel().setNodeDegrees(Calculate.nodeDegrees(getModel().getEdges().values()));
        val combinedEdgeCreationOrder = new ArrayList<List<Integer>>(getModel().getEdgeCreationOrder());
        if (getModel().getRemoteEdgeCreationOrder() != null)
        {
            for (val remoteEdgeCreationOrderPart : getModel().getRemoteEdgeCreationOrder())
            {
                combinedEdgeCreationOrder.add(Arrays.stream(remoteEdgeCreationOrderPart).boxed().collect(Collectors.toList()));
            }
        }

        val pathSummands = new ArrayList<Double>(getModel().getQueryPathLength());
        var pathSum = 0.0d;
        var first = true;

        val pathScores = new ArrayList<Float>();
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

            pathScores.add((float) pathSum);
        }

        publishPathScores(Floats.toArray(pathScores));
    }

    private double addSummands(List<Double> pathSummands, List<Integer> edgeCreationOrder, double currentPathSum)
    {
        var newPathSum = currentPathSum;
        for (val edgeHash : edgeCreationOrder)
        {
            val edge = getModel().getEdges().get(edgeHash);
            val nodeDegree = getModel().getNodeDegrees().get(edge.getFrom().hashCode()) - 1L;
            val summand = (edge.getWeight() * nodeDegree) / (double) getModel().getQueryPathLength();
            newPathSum += summand;
            pathSummands.add(summand);
        }

        return newPathSum;
    }

    private boolean isReadyToScoreSubSequences()
    {
        if (!getModel().isResponsibilitiesReceived())
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

    private void publishPathScores(float[] pathScores)
    {
        val protocol = getMasterProtocol(ProtocolType.Scoring);
        assert protocol.isPresent() : "The master processor must implement the Scoring protocol!";

        getModel().getDataTransferManager().transfer(GenericDataSource.create(pathScores),
                                                     dataDistributor -> ScoringMessages.InitializePathScoresTransferMessage.builder()
                                                                                                                           .sender(getModel().getSelf())
                                                                                                                           .receiver(protocol.get().getRootActor())
                                                                                                                           .operationId(dataDistributor.getOperationId())
                                                                                                                           .build());
    }
}
