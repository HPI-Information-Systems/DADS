package de.hpi.msc.jschneider.protocol.processorRegistration.processorRegistry;

import akka.actor.ActorSelection;
import akka.actor.Cancellable;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.messageExchange.AbstractMessageExchangeParticipantControl;
import de.hpi.msc.jschneider.protocol.processorRegistration.Processor;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorRegistrationEvents;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorRegistrationMessages;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorRegistrationProtocol;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import lombok.val;

import java.time.Duration;

public class ProcessorRegistryControl extends AbstractMessageExchangeParticipantControl<ProcessorRegistryModel>
{
    public ProcessorRegistryControl(ProcessorRegistryModel model)
    {
        super(model);
    }

    @Override
    public ImprovedReceiveBuilder complementReceiveBuilder(ImprovedReceiveBuilder builder)
    {
        return builder.match(ProcessorRegistrationMessages.RegisterAtMasterMessage.class, this::onRegisterAtMaster)
                      .match(ProcessorRegistrationMessages.AcknowledgeRegistrationMessage.class, this::onAcknowledgeRegistration)
                      .match(ProcessorRegistrationMessages.ProcessorRegistrationMessage.class, this::onRegistration);
    }

    private void onRegisterAtMaster(ProcessorRegistrationMessages.RegisterAtMasterMessage message)
    {
        cancelRegistrationSchedule();
        val nodeRegistry = trySelectActors(String.format("%1$s/user/%2$s", message.getMasterAddress(), ProcessorRegistrationProtocol.ROOT_ACTOR_NAME));
        if (nodeRegistry == null)
        {
            return;
        }

        getModel().setRegistrationSchedule(createRegistrationSchedule(nodeRegistry));
    }

    private void cancelRegistrationSchedule()
    {
        val schedule = getModel().getRegistrationSchedule();
        if (schedule == null)
        {
            return;
        }

        schedule.cancel();
    }

    private ActorSelection trySelectActors(String path)
    {
        try
        {
            return getModel().getActorSelectionCallback().apply(path);
        }
        catch (Exception exception)
        {
            getLog().error("Unable to select actors!", exception);
            return null;
        }
    }

    private Cancellable createRegistrationSchedule(ActorSelection masterProcessorRegistry)
    {
        val scheduler = getModel().getScheduler();
        val dispatcher = getModel().getDispatcher();

        if (scheduler == null || dispatcher == null)
        {
            return null;
        }

        val message = ProcessorRegistrationMessages.ProcessorRegistrationMessage.builder()
                                                                                .processor(getModel().getLocalProcessor())
                                                                                .build();

        return scheduler.scheduleAtFixedRate(Duration.ZERO,
                                             getModel().getResendRegistrationInterval(),
                                             () -> masterProcessorRegistry.tell(message, getModel().getSelf()),
                                             dispatcher);
    }

    private void onAcknowledgeRegistration(ProcessorRegistrationMessages.AcknowledgeRegistrationMessage message)
    {
        cancelRegistrationSchedule();
        getLog().info("Processor registration acknowledged.");
    }

    private void onRegistration(ProcessorRegistrationMessages.ProcessorRegistrationMessage message)
    {
        val rootPath = message.getProcessor().getRootPath();

        getModel().getProcessors().put(rootPath, message.getProcessor());
        val existingProcessors = getModel().getProcessors().values().toArray(new Processor[0]);


        getModel().getSender().tell(ProcessorRegistrationMessages.AcknowledgeRegistrationMessage.builder()
                                                                                                .existingProcessors(existingProcessors)
                                                                                                .build(), getModel().getSelf());

        getLocalProtocol(ProtocolType.ProcessorRegistration).ifPresent(protocol ->
                                                                       {
                                                                           send(ProcessorRegistrationEvents.ProcessorJoinedEvent.builder()
                                                                                                                                .sender(getModel().getSelf())
                                                                                                                                .receiver(protocol.getEventDispatcher())
                                                                                                                                .processor(message.getProcessor())
                                                                                                                                .build());
                                                                       });

    }
}
