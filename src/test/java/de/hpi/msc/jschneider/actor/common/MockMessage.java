package de.hpi.msc.jschneider.actor.common;

import akka.actor.ActorRef;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

import java.util.UUID;

@SuperBuilder @Getter @Setter
public class MockMessage implements Message
{
    private static final long serialVersionUID = 2511853284452384098L;
    @Builder.Default
    private UUID id = UUID.randomUUID();
    @Builder.Default
    private ActorRef sender = ActorRef.noSender();
    @Builder.Default
    private ActorRef receiver = ActorRef.noSender();
    @Builder.Default
    private int content = 1337;
}
