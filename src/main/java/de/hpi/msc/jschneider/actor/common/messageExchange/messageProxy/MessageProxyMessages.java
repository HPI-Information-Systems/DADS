package de.hpi.msc.jschneider.actor.common.messageExchange.messageProxy;

import de.hpi.msc.jschneider.actor.common.AbstractCompletableMessage;
import lombok.Getter;
import lombok.experimental.SuperBuilder;

import java.util.UUID;

public class MessageProxyMessages
{
    @SuperBuilder
    public static class MessageCompletedMessage extends AbstractCompletableMessage
    {
        private static final long serialVersionUID = -5493282421403131254L;
        @Getter
        private UUID acknowledgedMessageId;
    }

    @SuperBuilder
    public static class BackPressureMessage extends AbstractCompletableMessage
    {
        private static final long serialVersionUID = -3874703752827167203L;
    }
}
