package de.hpi.msc.jschneider.protocol.actorPool;

import de.hpi.msc.jschneider.protocol.actorPool.worker.WorkConsumer;
import de.hpi.msc.jschneider.protocol.actorPool.worker.WorkFactory;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

public class ActorPoolMessages
{
    @NoArgsConstructor @SuperBuilder @Getter
    public static class ExecuteDistributedFromFactoryMessage extends MessageExchangeMessages.MessageExchangeMessage
    {
        private static final long serialVersionUID = -5051662566998145495L;
        @NonNull
        private WorkFactory workFactory;
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static abstract class WorkMessage extends MessageExchangeMessages.RedirectableMessage
    {
        private static final long serialVersionUID = 7013182952983341680L;
        @NonNull
        WorkConsumer consumer;
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class WorkDoneMessage extends MessageExchangeMessages.MessageExchangeMessage
    {
        private static final long serialVersionUID = 4619509949308314479L;
    }
}
