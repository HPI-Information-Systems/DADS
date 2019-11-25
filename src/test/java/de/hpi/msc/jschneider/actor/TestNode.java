package de.hpi.msc.jschneider.actor;

import akka.actor.ActorSystem;
import akka.actor.RootActorPath;
import akka.testkit.TestProbe;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.val;

@Getter @Setter @Builder
public class TestNode
{
    private ActorSystem actorSystem;
    private TestProbe messageDispatcher;
    private TestProbe workDispatcher;

    public static TestNode create(String actorSystemName)
    {
        val actorSystem = ActorSystem.create(actorSystemName);

        return builder()
                .actorSystem(actorSystem)
                .messageDispatcher(TestProbe.apply("MessageDispatcher", actorSystem))
                .workDispatcher(TestProbe.apply("WorkDispatcher", actorSystem))
                .build();
    }

    public void terminate()
    {
        actorSystem.terminate();
    }

    public RootActorPath rootPath()
    {
        return messageDispatcher.ref().path().root();
    }
}
