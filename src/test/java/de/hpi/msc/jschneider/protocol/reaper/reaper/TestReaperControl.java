package de.hpi.msc.jschneider.protocol.reaper.reaper;

import akka.actor.Terminated;
import akka.testkit.TestProbe;
import de.hpi.msc.jschneider.protocol.ProtocolTestCase;
import de.hpi.msc.jschneider.protocol.TestProcessor;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.reaper.ReaperEvents;
import de.hpi.msc.jschneider.protocol.reaper.ReaperMessages;
import lombok.val;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

public class TestReaperControl extends ProtocolTestCase
{
    private TestProbe localActor;
    private TestProcessor remoteProcessor;
    private TestProbe remoteActor;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();

        localActor = localProcessor.createActor("actor");
        remoteProcessor = createProcessor("remote");
        remoteActor = remoteProcessor.createActor("actor");
    }

    @Override
    protected ProtocolType[] getProcessorProtocols()
    {
        return new ProtocolType[]{ProtocolType.Reaper, ProtocolType.MessageExchange};
    }

    private ReaperControl control()
    {
        return new ReaperControl(dummyModel());
    }

    private ReaperModel dummyModel()
    {
        return finalizeModel(ReaperModel.builder()
                                        .terminateActorSystemCallback(() ->
                                                                      {
                                                                      })
                                        .build());
    }

    private ReaperMessages.WatchMeMessage watchMe(TestProbe actor)
    {
        return ReaperMessages.WatchMeMessage.builder()
                                            .sender(actor.ref())
                                            .receiver(self.ref())
                                            .build();
    }

    public void testWatchLocalActor()
    {
        val control = control();
        val messageInterface = messageInterface(control);

        val message = watchMe(localActor);
        messageInterface.apply(message);

        assertThat(control.getModel().getWatchedActors().size()).isEqualTo(1);
        assertThat(control.getModel().getWatchedActors()).contains(localActor.ref());
        assertThatMessageIsCompleted(message);
    }

    public void testDoNotWatchRemoteActor()
    {
        val control = control();
        val messageInterface = messageInterface(control);

        val message = watchMe(remoteActor);
        messageInterface.apply(message);

        assertThat(control.getModel().getWatchedActors()).isEmpty();
        assertThatMessageIsCompleted(message);
    }

    public void testTerminateSystem()
    {
        val control = control();
        val messageInterface = messageInterface(control);
        val secondLocalActor = localProcessor.createActor("actor2");

        control.getModel().getWatchedActors().add(localActor.ref());
        control.getModel().getWatchedActors().add(secondLocalActor.ref());

        val numberOfCalls = new AtomicInteger();
        control.getModel().setTerminateActorSystemCallback(numberOfCalls::getAndIncrement);

        messageInterface.apply(new Terminated(localActor.ref(), true, true));
        assertThat(numberOfCalls.get()).isZero();

        messageInterface.apply(new Terminated(secondLocalActor.ref(), true, true));
        assertThat(numberOfCalls.get()).isEqualTo(1);

        localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(ReaperEvents.ActorSystemReapedEvents.class);
    }
}
