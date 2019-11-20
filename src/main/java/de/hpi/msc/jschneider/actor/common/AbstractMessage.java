package de.hpi.msc.jschneider.actor.common;

import akka.actor.ActorRef;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

@NoArgsConstructor @SuperBuilder @Getter
public abstract class AbstractMessage implements Message
{
    @NonNull
    private ActorRef sender;
    @NonNull
    private ActorRef receiver;
}