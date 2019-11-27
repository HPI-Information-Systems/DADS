package de.hpi.msc.jschneider.protocol;

import akka.actor.ActorRef;
import akka.testkit.TestProbe;
import de.hpi.msc.jschneider.protocol.common.Protocol;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.val;

@Builder @Getter @Setter
public class TestProtocol implements Protocol
{
    private ProtocolType type;
    private TestProbe rootActorTestProbe;
    private TestProbe eventDispatcherTestProbe;

    public ActorRef getRootActor()
    {
        return rootActorTestProbe.ref();
    }

    public ActorRef getEventDispatcher()
    {
        return eventDispatcherTestProbe.ref();
    }

    public static TestProtocol create(TestProcessor testProcessor, ProtocolType type)
    {
        val rootActor = TestProbe.apply(type + "RootActor", testProcessor.getActorSystem());
        val eventDispatcher = TestProbe.apply(type + "EventDispatcher", testProcessor.getActorSystem());

        val protocol = builder().type(type)
                                .rootActorTestProbe(rootActor)
                                .eventDispatcherTestProbe(eventDispatcher)
                                .build();

        testProcessor.addProtocol(protocol);

        return protocol;
    }
}
