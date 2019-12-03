package de.hpi.msc.jschneider.protocol.common.eventDispatcher;

import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

public class EventDispatcherMessages
{
    @NoArgsConstructor @SuperBuilder @Getter
    public static class SubscribeToEventMessage extends MessageExchangeMessages.MessageExchangeMessage
    {
        private static final long serialVersionUID = 7644079275799521967L;
        @NonNull
        private Class<? extends MessageExchangeMessages.RedirectableMessage> eventType;
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class UnsubscribeFromEventMessage extends MessageExchangeMessages.MessageExchangeMessage
    {
        private static final long serialVersionUID = 6007153563477079326L;
        @NonNull
        private Class<? extends MessageExchangeMessages.RedirectableMessage> eventType;
    }
}
