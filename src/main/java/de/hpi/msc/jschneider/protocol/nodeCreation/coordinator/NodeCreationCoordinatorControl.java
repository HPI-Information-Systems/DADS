package de.hpi.msc.jschneider.protocol.nodeCreation.coordinator;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import de.hpi.msc.jschneider.protocol.nodeCreation.NodeCreationEvents;
import de.hpi.msc.jschneider.protocol.nodeCreation.NodeCreationMessages;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import de.hpi.msc.jschneider.utility.Int32Range;
import de.hpi.msc.jschneider.utility.Int64Range;
import lombok.val;
import lombok.var;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class NodeCreationCoordinatorControl extends AbstractProtocolParticipantControl<NodeCreationCoordinatorModel>
{
    private static final double MAXIMUM_VALUE_SCALE_FACTOR = 1.2d;

    public NodeCreationCoordinatorControl(NodeCreationCoordinatorModel model)
    {
        super(model);
    }

    @Override
    public ImprovedReceiveBuilder complementReceiveBuilder(ImprovedReceiveBuilder builder)
    {
        return super.complementReceiveBuilder(builder)
                    .match(NodeCreationMessages.NodeCreationWorkerReadyMessage.class, this::onNodeCreationWorkerReady);
    }

    private void onNodeCreationWorkerReady(NodeCreationMessages.NodeCreationWorkerReadyMessage message)
    {
        try
        {
            val protocol = getProtocol(message.getSender().path().root(), ProtocolType.EdgeCreation);
            if (!protocol.isPresent())
            {
                getLog().error(String.format("Processor (%1$s) declared ready for node creation although edge creation is not supported!",
                                             message.getSender().path().root()));
                return;
            }

            getModel().getReadyMessages().add(message);
            getModel().setMaximumValue(Math.max(getModel().getMaximumValue(), message.getMaximumValue()));

            val sortedMessages = sortReadyMessages();
            if (!allWorkersReady(sortedMessages))
            {
                return;
            }

            initializeNodeCreation(sortedMessages);
        }
        finally
        {
            complete(message);
        }
    }

    private List<NodeCreationMessages.NodeCreationWorkerReadyMessage> sortReadyMessages()
    {
        val sortedList = new ArrayList<NodeCreationMessages.NodeCreationWorkerReadyMessage>(getModel().getReadyMessages());
        sortedList.sort((a, b) -> (int) (a.getSubSequenceIndices().getFrom() - b.getSubSequenceIndices().getFrom()));

        return sortedList;
    }

    private boolean allWorkersReady(List<NodeCreationMessages.NodeCreationWorkerReadyMessage> sortedMessages)
    {
        if (sortedMessages.size() < 1)
        {
            return false;
        }

        if (sortedMessages.get(0).getSubSequenceIndices().getFrom() != 0)
        {
            return false;
        }

        for (var i = 1; i < sortedMessages.size(); ++i)
        {
            val current = sortedMessages.get(i);
            val previous = sortedMessages.get(i - 1);

            if (current.getSubSequenceIndices().getFrom() != previous.getSubSequenceIndices().getTo())
            {
                return false;
            }
        }

        return sortedMessages.get(sortedMessages.size() - 1).isLastSubSequenceChunk();
    }

    private void initializeNodeCreation(List<NodeCreationMessages.NodeCreationWorkerReadyMessage> sortedMessages)
    {
        val messageTemplate = createInitializationMessage(sortedMessages);
        for (val worker : sortedMessages.stream().map(MessageExchangeMessages.MessageExchangeMessage::getSender).collect(Collectors.toList()))
        {
            send(messageTemplate.redirectTo(worker));
        }
    }

    private NodeCreationMessages.InitializeNodeCreationMessage createInitializationMessage(List<NodeCreationMessages.NodeCreationWorkerReadyMessage> sortedMessages)
    {
        val numberOfProcessors = sortedMessages.size();
        val numberOfSamplesPerProcessor = (int) Math.ceil(getModel().getTotalNumberOfIntersectionSegments() / (double) numberOfProcessors);

        val segmentResponsibilities = new HashMap<ActorRef, Int32Range>();
        val subSequenceResponsibilities = new HashMap<ActorRef, Int64Range>();
        var currentSampleStart = 0;
        for (val message : sortedMessages)
        {
            val end = Math.min(getModel().getTotalNumberOfIntersectionSegments(), currentSampleStart + numberOfSamplesPerProcessor);
            val sampleRange = Int32Range.builder()
                                        .from(currentSampleStart)
                                        .to(end)
                                        .build();
            currentSampleStart = end;

            segmentResponsibilities.put(message.getSender(), sampleRange);
            subSequenceResponsibilities.put(message.getSender(), message.getSubSequenceIndices());
        }

        return NodeCreationMessages.InitializeNodeCreationMessage.builder()
                                                                 .sender(getModel().getSelf())
                                                                 .receiver(getModel().getSelf())
                                                                 .numberOfIntersectionSegments(getModel().getTotalNumberOfIntersectionSegments())
                                                                 .maximumValue(getModel().getMaximumValue() * MAXIMUM_VALUE_SCALE_FACTOR)
                                                                 .intersectionSegmentResponsibilities(segmentResponsibilities)
                                                                 .subSequenceResponsibilities(subSequenceResponsibilities)
                                                                 .build();
    }
}
