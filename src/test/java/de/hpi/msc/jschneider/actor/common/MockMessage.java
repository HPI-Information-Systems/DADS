package de.hpi.msc.jschneider.actor.common;

import akka.actor.ActorRef;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

import java.util.UUID;

@SuperBuilder @Getter @Setter
public class MockMessage implements Message
{
    private static final long serialVersionUID = 2511853284452384098L;
    private UUID id = UUID.randomUUID();
    private ActorRef sender = ActorRef.noSender();
    private ActorRef receiver = ActorRef.noSender();
    private int content = 1337;
}
