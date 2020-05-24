package de.hpi.msc.jschneider.protocol.messageExchange;

import akka.actor.ActorRef;
import com.esotericsoftware.kryo.NotNull;
import de.hpi.msc.jschneider.utility.IdGenerator;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

import java.io.Serializable;

public class MessageExchangeMessages
{
    @NoArgsConstructor @SuperBuilder @Getter
    public static abstract class MessageExchangeMessage implements Serializable
    {
        private static final long serialVersionUID = -5714914705070819888L;
        @NotNull @Builder.Default
        private long id = IdGenerator.next();
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
        private long completedMessageId;
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class IntroduceMessageProxyMessage extends MessageExchangeMessage
    {
        private static final long serialVersionUID = -8420943951486193295L;
        @NonNull
        private ActorRef messageProxy;
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class UpdateRemoteMessageReceiverMessage implements Serializable
    {
        private static final long serialVersionUID = -9094788773578277980L;
        @NonNull
        private ActorRef remoteMessageReceiver;
    }
}
