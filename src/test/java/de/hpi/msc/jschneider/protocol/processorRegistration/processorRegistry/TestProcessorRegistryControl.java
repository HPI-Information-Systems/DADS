package de.hpi.msc.jschneider.protocol.processorRegistration.processorRegistry;

import de.hpi.msc.jschneider.protocol.ProtocolTestCase;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.processorRegistration.Processor;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorRegistrationEvents;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorRegistrationMessages;
import lombok.val;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

public class TestProcessorRegistryControl extends ProtocolTestCase
{
    @Override
    protected ProtocolType[] getProcessorProtocols()
    {
        return new ProtocolType[]{ProtocolType.ProcessorRegistration, ProtocolType.MessageExchange};
    }

    private ProcessorRegistryControl control()
    {
        return new ProcessorRegistryControl(dummyModel());
    }

    private ProcessorRegistryModel dummyModel()
    {
        val processorRegistry = localProcessor.getProtocolRootActor(ProtocolType.ProcessorRegistration);
        val selection = localProcessor.getActorSystem().actorSelection(processorRegistry.ref().path());

        return finalizeModel(ProcessorRegistryModel.builder()
                                                   .dispatcherProvider(() -> localProcessor.getActorSystem().dispatcher())
                                                   .schedulerProvider(() -> localProcessor.getActorSystem().scheduler())
                                                   .actorSelectionCallback(path -> selection)
                                                   .expectedNumberOfProcessors(1)
                                                   .build());
    }

    public void testRegisterAtMaster()
    {
        val control = control();
        val messageInterface = createMessageInterface(control);

        val message = ProcessorRegistrationMessages.RegisterAtMasterMessage.builder()
                                                                           .masterAddress(localProcessor.getActorSystem().provider().getDefaultAddress())
                                                                           .build();
        messageInterface.apply(message);

        val registrationMessage = localProcessor.getProtocolRootActor(ProtocolType.ProcessorRegistration).expectMsgClass(ProcessorRegistrationMessages.ProcessorRegistrationMessage.class);
        assertThat(registrationMessage.getProcessor()).isEqualTo(localProcessor);
    }

    public void testResendRegistrationMessage()
    {
        val control = control();
        control.getModel().setResendRegistrationInterval(Duration.ofMillis(100));
        val messageInterface = createMessageInterface(control);

        val message = ProcessorRegistrationMessages.RegisterAtMasterMessage.builder()
                                                                           .masterAddress(localProcessor.getActorSystem().provider().getDefaultAddress())
                                                                           .build();
        messageInterface.apply(message);

        val firstRegistrationMessage = localProcessor.getProtocolRootActor(ProtocolType.ProcessorRegistration).expectMsgClass(ProcessorRegistrationMessages.ProcessorRegistrationMessage.class);
        assertThat(firstRegistrationMessage.getProcessor()).isEqualTo(localProcessor);

        val secondRegistrationMessage = localProcessor.getProtocolRootActor(ProtocolType.ProcessorRegistration).expectMsgClass(ProcessorRegistrationMessages.ProcessorRegistrationMessage.class);
        assertThat(secondRegistrationMessage).isSameAs(firstRegistrationMessage);

        messageInterface.apply(ProcessorRegistrationMessages.AcknowledgeRegistrationMessage.builder()
                                                                                           .existingProcessors(new Processor[]{localProcessor})
                                                                                           .build());

        localProcessor.getProtocolRootActor(ProtocolType.ProcessorRegistration).expectNoMessage();
    }

    public void testOnRegistration()
    {
        val control = control();
        val messageInterface = createMessageInterface(control);

        val message = ProcessorRegistrationMessages.ProcessorRegistrationMessage.builder()
                                                                                .processor(localProcessor)
                                                                                .sender(self.ref())
                                                                                .build();
        messageInterface.apply(message);

        val acknowledgeRegistration = self.expectMsgClass(ProcessorRegistrationMessages.AcknowledgeRegistrationMessage.class);
        assertThat(acknowledgeRegistration.getExistingProcessors().length).isOne();
        assertThat(acknowledgeRegistration.getExistingProcessors()).contains(localProcessor);

        expectEvent(ProcessorRegistrationEvents.ProcessorJoinedEvent.class);
    }

    public void testSubscribeToEventsOnRegistrationAcknowledged()
    {
        val control = control();
        val messageInterface = createMessageInterface(control);

        val message = ProcessorRegistrationMessages.AcknowledgeRegistrationMessage.builder()
                                                                                  .existingProcessors(new Processor[]{localProcessor})
                                                                                  .build();
        messageInterface.apply(message);

        assertThat(control.getModel().getProcessors()).isNotEmpty();
        assertEventSubscription(ProcessorRegistrationEvents.ProcessorJoinedEvent.class);
        expectEvent(ProcessorRegistrationEvents.RegistrationAcknowledgedEvent.class);
    }
}
