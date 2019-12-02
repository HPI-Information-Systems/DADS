package de.hpi.msc.jschneider.protocol.processorRegistration;

import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

public class ProcessorRegistrationEvents
{
    @NoArgsConstructor @SuperBuilder @Getter
    public static class ProcessorJoinedEvent extends MessageExchangeMessages.MessageExchangeMessage
    {
        private static final long serialVersionUID = 1950315310184677324L;
        @NonNull
        private Processor processor;
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class RegistrationAcknowledgedEvent extends MessageExchangeMessages.MessageExchangeMessage
    {
        private static final long serialVersionUID = 2070149295388244936L;
    }
}
