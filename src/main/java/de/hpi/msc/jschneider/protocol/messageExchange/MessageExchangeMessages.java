package de.hpi.msc.jschneider.protocol.messageExchange;

import akka.actor.ActorRef;
import com.esotericsoftware.kryo.NotNull;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

public class MessageExchangeMessages
{
    private static final AtomicInteger ID = new AtomicInteger();

    @NoArgsConstructor @SuperBuilder @Getter
    public static abstract class MessageExchangeMessage implements Serializable
    {
        private static final long serialVersionUID = -5714914705070819888L;
        @NotNull @Builder.Default
        private int id = ID.incrementAndGet();
        @NonNull
        private ActorRef sender;
        @NonNull
        private ActorRef receiver;
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static abstract class RedirectableMessage extends MessageExchangeMessage
    {
        private static final long serialVersionUID = -2387997933538751338L;
        @Builder.Default
        private ActorRef forwarder = null;

        public abstract RedirectableMessage redirectTo(ActorRef newReceiver);
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
        private int completedMessageId;
    }
}
