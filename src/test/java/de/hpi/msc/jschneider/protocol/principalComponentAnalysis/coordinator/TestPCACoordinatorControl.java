package de.hpi.msc.jschneider.protocol.principalComponentAnalysis.coordinator;

import akka.actor.RootActorPath;
import de.hpi.msc.jschneider.protocol.ProtocolTestCase;
import de.hpi.msc.jschneider.protocol.TestProcessor;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.principalComponentAnalysis.PCAMessages;
import de.hpi.msc.jschneider.protocol.processorRegistration.Processor;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorRegistrationEvents;
import lombok.val;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class TestPCACoordinatorControl extends ProtocolTestCase
{
    private TestProcessor remoteProcessor;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        remoteProcessor = createProcessor("remote");
    }

    @Override
    protected ProtocolType[] getProcessorProtocols()
    {
        return new ProtocolType[]{ProtocolType.MessageExchange, ProtocolType.PrincipalComponentAnalysis, ProtocolType.ProcessorRegistration};
    }

    private PCACoordinatorModel dummyModel()
    {
        return finalizeModel(PCACoordinatorModel.builder()
                                                .numberOfParticipants(2)
                                                .build());
    }

    private PCACoordinatorControl control()
    {
        return new PCACoordinatorControl(dummyModel());
    }

    private ProcessorRegistrationEvents.ProcessorJoinedEvent processorJoined(Processor processor)
    {
        return ProcessorRegistrationEvents.ProcessorJoinedEvent.builder()
                                                               .sender(localProcessor.getProtocolRootActor(ProtocolType.ProcessorRegistration).ref())
                                                               .receiver(self.ref())
                                                               .processor(processor)
                                                               .build();
    }

    public void testProcessorJoined()
    {
        val control = control();
        val messageInterface = messageInterface(control);

        val processorJoinedEvent = processorJoined(localProcessor);
        messageInterface.apply(processorJoinedEvent);

        assertThat(control.getModel().getNextParticipantIndex().get()).isEqualTo(1);
        assertThat(control.getModel().getParticipantIndices().size()).isEqualTo(1);
        assertThat(control.getModel().getParticipantIndices()).containsValue(localProcessor.getRootPath());

        assertThatMessageIsCompleted(processorJoinedEvent);
    }

    public void testInitializeCalculationWhenAllProcessorsJoined()
    {
        val control = control();
        val messageInterface = messageInterface(control);

        val localProcessorJoinedEvent = processorJoined(localProcessor);
        messageInterface.apply(localProcessorJoinedEvent);

        assertThatMessageIsCompleted(localProcessorJoinedEvent);

        val remoteProcessorJoinedEvent = processorJoined(remoteProcessor);
        messageInterface.apply(remoteProcessorJoinedEvent);

        val initializationMessages = new ArrayList<PCAMessages.InitializePCACalculationMessage>();
        initializationMessages.add(localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(PCAMessages.InitializePCACalculationMessage.class));
        initializationMessages.add(localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(PCAMessages.InitializePCACalculationMessage.class));

        assertThat(initializationMessages.stream().filter(message -> message.getReceiver().equals(localProcessor.getProtocolRootActor(ProtocolType.PrincipalComponentAnalysis).ref())).count()).isEqualTo(1L);
        assertThat(initializationMessages.stream().filter(message -> message.getReceiver().equals(remoteProcessor.getProtocolRootActor(ProtocolType.PrincipalComponentAnalysis).ref())).count()).isEqualTo(1L);
        assertThat(initializationMessages.get(0).getProcessorIndices()).containsExactly(new AbstractMap.SimpleEntry<>(0L, localProcessor.getRootPath()), new AbstractMap.SimpleEntry<>(1L, remoteProcessor.getRootPath()));


        assertThatMessageIsCompleted(remoteProcessorJoinedEvent);
    }

    public void testIgnoreProcessorsWithoutExpectedProtocol()
    {
        val control = control();
        val messageInterface = messageInterface(control);

        val observerProcessor = createProcessor("observer", ProtocolType.MessageExchange, ProtocolType.ProcessorRegistration);
        val processorJoined = processorJoined(observerProcessor);
        messageInterface.apply(processorJoined);

        assertThat(control.getModel().getParticipantIndices()).isEmpty();
        assertThat(control.getModel().getNextParticipantIndex().get()).isEqualTo(0);

        assertThatMessageIsCompleted(processorJoined);
    }
}
