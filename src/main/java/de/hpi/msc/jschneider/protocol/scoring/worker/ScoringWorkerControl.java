package de.hpi.msc.jschneider.protocol.scoring.worker;

import akka.actor.RootActorPath;
import com.google.common.primitives.Ints;
import de.hpi.msc.jschneider.math.Calculate;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.edgeCreation.EdgeCreationEvents;
import de.hpi.msc.jschneider.protocol.graphMerging.GraphMergingEvents;
import de.hpi.msc.jschneider.protocol.nodeCreation.NodeCreationEvents;
import de.hpi.msc.jschneider.protocol.scoring.ScoringMessages;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import de.hpi.msc.jschneider.utility.Int64Range;
import lombok.val;
import lombok.var;

import java.util.ArrayList;
import java.util.Arrays;
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

    private void setProcessorResponsibleForPreviousSubSequences(Map<RootActorPath, Int64Range> subSequenceResponsibilities)
    {
        val myResponsibilities = subSequenceResponsibilities.get(getModel().getSelf().path().root());
        assert myResponsibilities != null : "We are not responsible for any sub sequences!";

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
            assert getModel().getEdgeCreationOrder().isEmpty() : "Edge creation order was received already!";

            getModel().getEdgeCreationOrder().addAll(Arrays.stream(message.getEdgeCreationOrder()).boxed().collect(Collectors.toList()));
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

        val overlappingEdgeCreationOrder = Ints.toArray(getModel().getEdgeCreationOrder().subList(0, getModel().getQueryPathLength() - 1));
        send(ScoringMessages.OverlappingEdgeCreationOrder.builder()
                                                         .sender(getModel().getSelf())
                                                         .receiver(getModel().getProcessorResponsibleForPreviousSubSequences())
                                                         .overlappingEdgeCreationOrder(overlappingEdgeCreationOrder)
                                                         .build());
    }

    private boolean isReadyToSendEdgeCreationOrderToPreviousResponsibleProcessor()
    {
        return getModel().getProcessorResponsibleForPreviousSubSequences() != null &&
               !getModel().getEdgeCreationOrder().isEmpty() &&
               getModel().getQueryPathLength() > 0;
    }

    private void onOverlappingEdgeCreationOrder(ScoringMessages.OverlappingEdgeCreationOrder message)
    {
        try
        {
            assert getModel().getRemoteEdgeCreationOrder().isEmpty() : "Overlapping edge creation order already received!";

            getModel().getRemoteEdgeCreationOrder().addAll(Arrays.stream(message.getOverlappingEdgeCreationOrder()).boxed().collect(Collectors.toList()));
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

        val nodeIndegrees = Calculate.nodeIndegrees(getModel().getEdges().values());
        val combinedEdgeCreationOrder = new ArrayList<Integer>();
        combinedEdgeCreationOrder.addAll(getModel().getEdgeCreationOrder());
        combinedEdgeCreationOrder.addAll(getModel().getRemoteEdgeCreationOrder());

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
                    val edgeHash = combinedEdgeCreationOrder.get(pathStartIndex + edgeIndex);
                    val edge = getModel().getEdges().get(edgeHash);
                    val nodeIndegree = nodeIndegrees.get(edge.getTo().hashCode()) - 1L;
                    val summand = (edge.getWeight() * nodeIndegree) / (double) getModel().getQueryPathLength();
                    pathSum += summand;
                    pathSummands.add(summand);
                }
            }
            else
            {
                pathSum -= pathSummands.remove(0);
                val edgeHash = combinedEdgeCreationOrder.get(pathStartIndex + getModel().getQueryPathLength() - 1);
                val edge = getModel().getEdges().get(edgeHash);
                val nodeIndegree = nodeIndegrees.get(edge.getTo().hashCode()) - 1L;
                val summand = (edge.getWeight() * nodeIndegree) / (double) getModel().getQueryPathLength();
                pathSum += summand;
                pathSummands.add(summand);
            }

            pathScores.add((float) pathSum);
        }
    }

    private boolean isReadyToScoreSubSequences()
    {
        return getModel().isResponsibilitiesReceived() &&
               (getModel().getProcessorResponsibleForPreviousSubSequences() == null || !getModel().getRemoteEdgeCreationOrder().isEmpty()) &&
               getModel().getQueryPathLength() > 0 &&
               !getModel().getEdgeCreationOrder().isEmpty() &&
               getModel().getEdges() != null;
    }
}
