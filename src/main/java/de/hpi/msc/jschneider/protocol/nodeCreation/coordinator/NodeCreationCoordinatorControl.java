package de.hpi.msc.jschneider.protocol.nodeCreation.coordinator;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.SystemParameters;
import de.hpi.msc.jschneider.bootstrap.command.MasterCommand;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import de.hpi.msc.jschneider.protocol.nodeCreation.NodeCreationMessages;
import de.hpi.msc.jschneider.protocol.processorRegistration.Processor;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorId;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import de.hpi.msc.jschneider.utility.Int32Range;
import de.hpi.msc.jschneider.utility.Int64Range;
import lombok.val;
import lombok.var;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
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
                getLog().error("Processor ({}) declared ready for node creation although edge creation is not supported!",
                               ProcessorId.of(message.getSender()));
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
        assert SystemParameters.getCommand() instanceof MasterCommand : "Only the master is able to initialize the node creation!";

        NodeCreationMessages.InitializeNodeCreationMessage message = null;
        switch (((MasterCommand) SystemParameters.getCommand()).getDistributionStrategy())
        {
            case HOMOGENEOUS:
            {
                message = createHomogeneousInitializationMessage(sortedMessages);
                break;
            }
            case HETEROGENEOUS:
            {
                message = createHeterogeneousInitializeMessage(sortedMessages);
                break;
            }
            default:
            {
                throw new IllegalArgumentException("Unknown distribution strategy!");
            }
        }

        for (val worker : sortedMessages.stream().map(MessageExchangeMessages.MessageExchangeMessage::getSender).collect(Collectors.toList()))
        {
            send(message.redirectTo(worker));
        }
    }

    private NodeCreationMessages.InitializeNodeCreationMessage createHomogeneousInitializationMessage(List<NodeCreationMessages.NodeCreationWorkerReadyMessage> sortedMessages)
    {
        val numberOfProcessors = sortedMessages.size();
        val numberOfSamplesPerProcessor = (int) Math.floor(getModel().getTotalNumberOfIntersectionSegments() / (double) numberOfProcessors);

        val segmentResponsibilities = new HashMap<ActorRef, Int32Range>();
        val subSequenceResponsibilities = new HashMap<ActorRef, Int64Range>();
        var currentSampleStart = 0;
        for (val message : sortedMessages)
        {
            var end = Math.min(getModel().getTotalNumberOfIntersectionSegments(), currentSampleStart + numberOfSamplesPerProcessor);
            if (segmentResponsibilities.size() == numberOfProcessors - 1)
            {
                // last responsibility
                end = getModel().getTotalNumberOfIntersectionSegments();
            }
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

    private NodeCreationMessages.InitializeNodeCreationMessage createHeterogeneousInitializeMessage(List<NodeCreationMessages.NodeCreationWorkerReadyMessage> sortedMessages)
    {
        val numberOfProcessors = sortedMessages.size();

        assert numberOfProcessors == getModel().getProcessors().length : "All processors should be involved in the node creation process!";

        val totalClusterMemory = Arrays.stream(getModel().getProcessors()).mapToLong(Processor::getMaximumMemoryInBytes).sum();
        val sortedProcessors = Arrays.stream(getModel().getProcessors()).sorted(Comparator.comparingLong(Processor::getMaximumMemoryInBytes)).iterator();
        val messageByProcessor = sortedMessages.stream().collect(Collectors.toMap(message -> ProcessorId.of(message.getSender()), message -> message));

        val segmentResponsibilities = new HashMap<ActorRef, Int32Range>(numberOfProcessors);
        val sequenceResponsibilities = new HashMap<ActorRef, Int64Range>(numberOfProcessors);

        var nextIntersectionSegment = 0;
        while (sortedProcessors.hasNext())
        {
            val processor = sortedProcessors.next();
            val message = messageByProcessor.get(processor.getId());

            val memoryShare = processor.getMaximumMemoryInBytes() / (double) totalClusterMemory;
            var lastSegment = (int) Math.min(getModel().getTotalNumberOfIntersectionSegments(), nextIntersectionSegment + Math.floor(getModel().getTotalNumberOfIntersectionSegments() * memoryShare));
            if (!sortedProcessors.hasNext())
            {
                lastSegment = getModel().getTotalNumberOfIntersectionSegments();
            }

            val segmentRange = Int32Range.builder()
                                         .from(nextIntersectionSegment)
                                         .to(lastSegment)
                                         .build();
            nextIntersectionSegment = lastSegment;

            segmentResponsibilities.put(message.getSender(), segmentRange);
            sequenceResponsibilities.put(message.getSender(), message.getSubSequenceIndices());
        }

        return NodeCreationMessages.InitializeNodeCreationMessage.builder()
                                                                 .sender(getModel().getSelf())
                                                                 .receiver(getModel().getSelf())
                                                                 .numberOfIntersectionSegments(getModel().getTotalNumberOfIntersectionSegments())
                                                                 .maximumValue(getModel().getMaximumValue() * MAXIMUM_VALUE_SCALE_FACTOR)
                                                                 .intersectionSegmentResponsibilities(segmentResponsibilities)
                                                                 .subSequenceResponsibilities(sequenceResponsibilities)
                                                                 .build();
    }
}
