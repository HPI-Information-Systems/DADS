package de.hpi.msc.jschneider.actor.common;

import akka.actor.ActorRef;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

import java.util.UUID;

@NoArgsConstructor @SuperBuilder @Getter
public abstract class AbstractMessage implements Message
{
    @NonNull @Getter
    private final UUID id = UUID.randomUUID();
    @NonNull
    private ActorRef sender;
    @NonNull
    private ActorRef receiver;
}
