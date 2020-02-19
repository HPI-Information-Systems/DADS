package de.hpi.msc.jschneider.protocol.processorRegistration.processorRegistry;

import akka.actor.ActorSelection;
import akka.actor.Address;
import akka.actor.Cancellable;
import de.hpi.msc.jschneider.protocol.common.CommonMessages;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.processorRegistration.Processor;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorRegistrationEvents;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorRegistrationMessages;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorRegistrationProtocol;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import lombok.val;

import java.time.Duration;

public class ProcessorRegistryControl extends AbstractProtocolParticipantControl<ProcessorRegistryModel>
{
    public ProcessorRegistryControl(ProcessorRegistryModel model)
    {
        super(model);
    }

    @Override
    public ImprovedReceiveBuilder complementReceiveBuilder(ImprovedReceiveBuilder builder)
    {
        return super.complementReceiveBuilder(builder)
                    .match(CommonMessages.SetUpProtocolMessage.class, this::onSetUp)
                    .match(ProcessorRegistrationMessages.RegisterAtMasterMessage.class, this::onRegisterAtMaster)
                    .match(ProcessorRegistrationMessages.AcknowledgeRegistrationMessage.class, this::onAcknowledgeRegistration)
                    .match(ProcessorRegistrationMessages.ProcessorRegistrationMessage.class, this::onRegistration)
                    .match(ProcessorRegistrationEvents.ProcessorJoinedEvent.class, this::onProcessorJoined);
    }

    private void onSetUp(CommonMessages.SetUpProtocolMessage message)
    {

    }

    private void onRegisterAtMaster(ProcessorRegistrationMessages.RegisterAtMasterMessage message)
    {
        cancelRegistrationSchedule();
        getModel().setRegistrationSchedule(createRegistrationSchedule(message.getMasterAddress()));
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

    private Cancellable createRegistrationSchedule(Address masterAddress)
    {
        val scheduler = getModel().getScheduler();
        val dispatcher = getModel().getDispatcher();

        if (scheduler == null || dispatcher == null)
        {
            return null;
        }

        val message = ProcessorRegistrationMessages.ProcessorRegistrationMessage.builder()
                                                                                .processor(getModel().getLocalProcessor())
                                                                                .sender(getModel().getSelf())
                                                                                .build();

        return scheduler.scheduleAtFixedRate(Duration.ZERO,
                                             getModel().getResendRegistrationInterval(),
                                             () ->
                                             {
                                                 val masterNodeRegistry = trySelectActors(String.format("%1$s/user/%2$s", masterAddress, ProcessorRegistrationProtocol.ROOT_ACTOR_NAME));
                                                 if (masterNodeRegistry == null)
                                                 {
                                                     return;
                                                 }
                                                 masterNodeRegistry.tell(message, getModel().getSelf());
                                             },
                                             dispatcher);
    }

    private void onAcknowledgeRegistration(ProcessorRegistrationMessages.AcknowledgeRegistrationMessage message)
    {
        cancelRegistrationSchedule();
        getLog().info("Processor registration acknowledged.");

        for (val processor : message.getExistingProcessors())
        {
            getModel().getClusterProcessors().put(processor.getId(), processor);
        }

        subscribeToMasterEvent(ProtocolType.ProcessorRegistration, ProcessorRegistrationEvents.ProcessorJoinedEvent.class);
        trySendEvent(ProtocolType.ProcessorRegistration, eventDispatcher -> ProcessorRegistrationEvents.RegistrationAcknowledgedEvent.builder()
                                                                                                                                     .sender(getModel().getSelf())
                                                                                                                                     .receiver(eventDispatcher)
                                                                                                                                     .build());
    }

    private void onProcessorJoined(ProcessorRegistrationEvents.ProcessorJoinedEvent message)
    {
        try
        {
            getModel().getClusterProcessors().put(message.getProcessor().getId(), message.getProcessor());
            getLog().info(String.format("%1$s just joined.", message.getProcessor().getId()));

            if (!getModel().getLocalProcessor().isMaster())
            {
                trySendEvent(ProtocolType.ProcessorRegistration, eventDispatcher -> ProcessorRegistrationEvents.ProcessorJoinedEvent.builder()
                                                                                                                                    .sender(getModel().getSelf())
                                                                                                                                    .receiver(eventDispatcher)
                                                                                                                                    .processor(message.getProcessor())
                                                                                                                                    .build());
            }
        }
        finally
        {
            complete(message);
        }
    }

    private void onRegistration(ProcessorRegistrationMessages.ProcessorRegistrationMessage message)
    {
        val processorId = message.getProcessor().getId();
        getModel().getRegistrationMessages().put(processorId, message);
        getModel().getClusterProcessors().put(processorId, message.getProcessor());

        if (getModel().getRegistrationMessages().size() > getModel().getExpectedNumberOfProcessors())
        {
            getLog().warn(String.format("Received registration from unexpected processor (id = %1$s).", processorId));
            return;
        }

        if (getModel().getRegistrationMessages().size() != getModel().getExpectedNumberOfProcessors())
        {
            // wait for all expected nodes to register first
            return;
        }


        acknowledgeRegistrationMessages();
    }

    private void acknowledgeRegistrationMessages()
    {
        val existingProcessors = getModel().getClusterProcessors().values().toArray(new Processor[0]);

        val masterMessage = getModel().getRegistrationMessages().entrySet()
                                      .stream()
                                      .filter(entry -> entry.getValue().getProcessor().isMaster())
                                      .findFirst();

        assert masterMessage.isPresent() : "Master did not register itself!";

        if (getModel().getAcknowledgedRegistrationMessages().add(masterMessage.get().getKey()))
        {
            acknowledgeRegistrationMessage(masterMessage.get().getValue(), existingProcessors);
        }

        for (val entry : getModel().getRegistrationMessages().entrySet())
        {
            if (!getModel().getAcknowledgedRegistrationMessages().add(entry.getKey()))
            {
                continue;
            }

            val message = entry.getValue();
            acknowledgeRegistrationMessage(message, existingProcessors);
        }
    }

    private void acknowledgeRegistrationMessage(ProcessorRegistrationMessages.ProcessorRegistrationMessage message, Processor[] processors)
    {
        message.getSender().tell(ProcessorRegistrationMessages.AcknowledgeRegistrationMessage.builder()
                                                                                             .existingProcessors(processors)
                                                                                             .build(),
                                 getModel().getSelf());

        trySendEvent(ProtocolType.ProcessorRegistration, eventDispatcher -> ProcessorRegistrationEvents.ProcessorJoinedEvent.builder()
                                                                                                                            .sender(getModel().getSelf())
                                                                                                                            .receiver(eventDispatcher)
                                                                                                                            .processor(message.getProcessor())
                                                                                                                            .build());
    }
}
