package de.hpi.msc.jschneider.actor.common.messageExchange.messageProxy;

import de.hpi.msc.jschneider.actor.common.AbstractMessage;
import lombok.Getter;
import lombok.experimental.SuperBuilder;

import java.util.UUID;

public class MessageProxyMessages
{
    @SuperBuilder
    public static class MessageCompletedMessage extends AbstractMessage
    {
        private static final long serialVersionUID = -5493282421403131254L;
        @Getter
        private UUID acknowledgedMessageId;
    }
}
