package de.hpi.msc.jschneider.protocol.processorRegistration;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

public class ProcessorRegistrationEvents
{
    @NoArgsConstructor @SuperBuilder @Getter
    public static class ProcessorJoinedEvent extends MessageExchangeMessages.RedirectableMessage
    {
        private static final long serialVersionUID = 1950315310184677324L;
        @NonNull
        private Processor processor;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .forwarder(getReceiver())
                            .processor(getProcessor())
                            .build();
        }
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class RegistrationAcknowledgedEvent extends MessageExchangeMessages.RedirectableMessage
    {
        private static final long serialVersionUID = 2070149295388244936L;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .forwarder(getReceiver())
                            .build();
        }
    }
}
