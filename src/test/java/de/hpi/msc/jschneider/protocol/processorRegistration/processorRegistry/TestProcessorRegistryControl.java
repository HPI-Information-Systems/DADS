package de.hpi.msc.jschneider.protocol.processorRegistration.processorRegistry;

import de.hpi.msc.jschneider.protocol.ProtocolTestCase;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorRegistrationMessages;
import lombok.val;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

public class TestProcessorRegistryControl extends ProtocolTestCase
{
    @Override
    protected ProtocolType[] getProcessorProtocols()
    {
        return new ProtocolType[]{ProtocolType.ProcessorRegistration};
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
                                                   .localProcessor(localProcessor)
                                                   .dispatcherProvider(() -> localProcessor.getActorSystem().dispatcher())
                                                   .schedulerProvider(() -> localProcessor.getActorSystem().scheduler())
                                                   .actorSelectionCallback(path -> selection)
                                                   .build());
    }

    public void testRegisterAtMaster()
    {
        val control = control();
        val messageInterface = messageInterface(control);

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
        val messageInterface = messageInterface(control);

        val message = ProcessorRegistrationMessages.RegisterAtMasterMessage.builder()
                                                                           .masterAddress(localProcessor.getActorSystem().provider().getDefaultAddress())
                                                                           .build();
        messageInterface.apply(message);

        val firstRegistrationMessage = localProcessor.getProtocolRootActor(ProtocolType.ProcessorRegistration).expectMsgClass(ProcessorRegistrationMessages.ProcessorRegistrationMessage.class);
        assertThat(firstRegistrationMessage.getProcessor()).isEqualTo(localProcessor);

        val secondRegistrationMessage = localProcessor.getProtocolRootActor(ProtocolType.ProcessorRegistration).expectMsgClass(ProcessorRegistrationMessages.ProcessorRegistrationMessage.class);
        assertThat(secondRegistrationMessage).isSameAs(firstRegistrationMessage);

        messageInterface.apply(ProcessorRegistrationMessages.AcknowledgeRegistrationMessage.builder().build());

        localProcessor.getProtocolRootActor(ProtocolType.ProcessorRegistration).expectNoMessage();
    }
}
