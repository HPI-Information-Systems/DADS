package de.hpi.msc.jschneider.protocol.nodeCreation.coordinator;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import de.hpi.msc.jschneider.protocol.nodeCreation.NodeCreationMessages;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import de.hpi.msc.jschneider.utility.Int32Range;
import lombok.val;
import lombok.var;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
        sortedList.sort((a, b) -> (int) (a.getSubSequenceIndices().getStart() - b.getSubSequenceIndices().getStart()));

        return sortedList;
    }

    private boolean allWorkersReady(List<NodeCreationMessages.NodeCreationWorkerReadyMessage> sortedMessages)
    {
        if (sortedMessages.size() < 1)
        {
            return false;
        }

        if (sortedMessages.get(0).getSubSequenceIndices().getStart() != 0)
        {
            return false;
        }

        for (var i = 1; i < sortedMessages.size(); ++i)
        {
            val current = sortedMessages.get(i);
            val previous = sortedMessages.get(i - 1);

            if (current.getSubSequenceIndices().getStart() != previous.getSubSequenceIndices().getEnd())
            {
                return false;
            }
        }

        return sortedMessages.get(sortedMessages.size() - 1).isLastSubSequenceChunk();
    }

    private void initializeNodeCreation(List<NodeCreationMessages.NodeCreationWorkerReadyMessage> sortedMessages)
    {
        val responsibilities = createSampleResponsibilities(sortedMessages);
        for (val worker : sortedMessages.stream().map(MessageExchangeMessages.MessageExchangeMessage::getSender).collect(Collectors.toList()))
        {
            send(NodeCreationMessages.InitializeNodeCreationMessage.builder()
                                                                   .sender(getModel().getSelf())
                                                                   .receiver(worker)
                                                                   .numberOfSamples(getModel().getTotalNumberOfSamples())
                                                                   .maximumValue(getModel().getMaximumValue() * MAXIMUM_VALUE_SCALE_FACTOR)
                                                                   .sampleResponsibilities(responsibilities)
                                                                   .build());
        }
    }

    private Map<ActorRef, Int32Range> createSampleResponsibilities(List<NodeCreationMessages.NodeCreationWorkerReadyMessage> sortedMessages)
    {
        val numberOfProcessors = sortedMessages.size();
        val numberOfSamplesPerProcessor = (int) Math.ceil(getModel().getTotalNumberOfSamples() / (double) numberOfProcessors);

        val responsibilities = new HashMap<ActorRef, Int32Range>();
        var currentSampleStart = 0;
        for (val sortedMessage : sortedMessages)
        {
            val end = Math.min(getModel().getTotalNumberOfSamples(), currentSampleStart + numberOfSamplesPerProcessor);
            val sampleRange = Int32Range.builder()
                                        .start(currentSampleStart)
                                        .end(end)
                                        .build();
            currentSampleStart = end;

            responsibilities.put(sortedMessage.getSender(), sampleRange);
        }

        return responsibilities;
    }
}
