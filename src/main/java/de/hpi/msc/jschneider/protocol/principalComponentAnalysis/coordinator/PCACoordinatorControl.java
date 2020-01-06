package de.hpi.msc.jschneider.protocol.principalComponentAnalysis.coordinator;

import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.principalComponentAnalysis.PCAMessages;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorRegistrationEvents;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import lombok.val;

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
            val protocol = getProtocol(message.getProcessor().getRootPath(), ProtocolType.PrincipalComponentAnalysis);
            if (!protocol.isPresent())
            {
                return;
            }

            if (getModel().getParticipantIndices().size() >= getModel().getNumberOfParticipants())
            {
                return;
            }

            if (getModel().getParticipantIndices().containsValue(message.getProcessor().getRootPath()))
            {
                // processor re-joined?!
                return;
            }

            val processorIndex = getModel().getNextParticipantIndex().getAndIncrement();
            getModel().getParticipantIndices().put(processorIndex, message.getProcessor().getRootPath());

            if (getModel().getParticipantIndices().size() != getModel().getNumberOfParticipants())
            {
                return;
            }

            getLog().info(String.format("Initializing PCA calculation with %1$d participants.", getModel().getNumberOfParticipants()));
            initializeCalculation();
        }
        finally
        {
            complete(message);
        }
    }

    private void initializeCalculation()
    {
        for (val participantRootPath : getModel().getParticipantIndices().values())
        {
            getProtocol(participantRootPath, ProtocolType.PrincipalComponentAnalysis).ifPresent(protocol ->
                                                                                                {
                                                                                                    send(PCAMessages.InitializePCACalculationMessage.builder()
                                                                                                                                                    .sender(getModel().getSelf())
                                                                                                                                                    .receiver(protocol.getRootActor())
                                                                                                                                                    .processorIndices(getModel().getParticipantIndices())
                                                                                                                                                    .build());
                                                                                                });
        }
    }
}
