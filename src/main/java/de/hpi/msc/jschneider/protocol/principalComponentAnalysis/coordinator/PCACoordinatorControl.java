package de.hpi.msc.jschneider.protocol.principalComponentAnalysis.coordinator;

import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.principalComponentAnalysis.PCAMessages;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorId;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorRegistrationEvents;
import de.hpi.msc.jschneider.utility.Counter;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import lombok.val;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class PCACoordinatorControl extends AbstractProtocolParticipantControl<PCACoordinatorModel>
{
    public PCACoordinatorControl(PCACoordinatorModel model)
    {
        super(model);
    }

    @Override
    public void preStart()
    {
        super.preStart();
        if (!getModel().getLocalProcessor().isMaster())
        {
            return;
        }

        subscribeToLocalEvent(ProtocolType.ProcessorRegistration, ProcessorRegistrationEvents.ProcessorJoinedEvent.class);
    }

    @Override
    public ImprovedReceiveBuilder complementReceiveBuilder(ImprovedReceiveBuilder builder)
    {
        return super.complementReceiveBuilder(builder)
                    .match(ProcessorRegistrationEvents.ProcessorJoinedEvent.class, this::onProcessorJoined);
    }

    private void onProcessorJoined(ProcessorRegistrationEvents.ProcessorJoinedEvent message)
    {
        try
        {
            val protocol = getProtocol(message.getProcessor().getId(), ProtocolType.PrincipalComponentAnalysis);
            if (!protocol.isPresent())
            {
                return;
            }

            getModel().getParticipants().add(message.getProcessor().getId());

            if (getModel().getParticipants().size() != getModel().getNumberOfParticipants())
            {
                return;
            }

            getLog().info("Initializing PCA calculation with {} participants.", getModel().getNumberOfParticipants());
            initializeCalculation();
        }
        finally
        {
            complete(message);
        }
    }

    private void initializeCalculation()
    {
        val participantIndicesReversed = new HashMap<ProcessorId, Long>();
        val nextParticipantIndex = new Counter(0L);
        participantIndicesReversed.put(ProcessorId.of(getModel().getSelf()), nextParticipantIndex.getAndIncrement());

        for (val participant : getModel().getParticipants())
        {
            if (participantIndicesReversed.containsKey(participant))
            {
                continue;
            }

            participantIndicesReversed.put(participant, nextParticipantIndex.getAndIncrement());
        }

        assert participantIndicesReversed.size() == getModel().getNumberOfParticipants() : "Number of participants was unexpected!";
        val participantIndices = participantIndicesReversed.entrySet()
                                                           .stream()
                                                           .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));

        for (val participant : participantIndices.values())
        {
            getProtocol(participant, ProtocolType.PrincipalComponentAnalysis)
                    .ifPresent(protocol ->
                               {
                                   send(PCAMessages.InitializePCACalculationMessage.builder()
                                                                                   .sender(getModel().getSelf())
                                                                                   .receiver(protocol.getRootActor())
                                                                                   .processorIndices(participantIndices)
                                                                                   .build());
                               });
        }
    }
}
