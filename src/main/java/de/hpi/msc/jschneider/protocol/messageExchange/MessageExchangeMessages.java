package de.hpi.msc.jschneider.protocol.messageExchange;

import akka.actor.ActorRef;
import com.esotericsoftware.kryo.NotNull;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

import java.io.Serializable;
import java.util.UUID;

public class MessageExchangeMessages
{
    @NoArgsConstructor @SuperBuilder @Getter
    public static abstract class MessageExchangeMessage implements Serializable
    {
        private static final long serialVersionUID = -5714914705070819888L;
        @NotNull @Builder.Default
        private UUID id = UUID.randomUUID();
        @NotNull
        private ActorRef sender;
        @NotNull
        private ActorRef receiver;
    }

    @NoArgsConstructor @SuperBuilder
    public static class BackPressureMessage extends MessageExchangeMessage
    {
        private static final long serialVersionUID = -8941937816057218647L;
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class MessageCompletedMessage extends MessageExchangeMessage
    {
        private static final long serialVersionUID = -7753147974709076497L;
        @NonNull
        private UUID completedMessageId;
    }
}
