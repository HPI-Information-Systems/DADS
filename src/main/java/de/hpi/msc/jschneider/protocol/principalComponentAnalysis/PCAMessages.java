package de.hpi.msc.jschneider.protocol.principalComponentAnalysis;

import akka.actor.ActorRef;
import akka.actor.RootActorPath;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.util.HashMap;
import java.util.Map;

public class PCAMessages
{
    @NoArgsConstructor @SuperBuilder @Getter
    public static class InitializePCACalculationMessage extends MessageExchangeMessages.RedirectableMessage
    {
        private static final long serialVersionUID = 7959217715683067548L;
        private Map<RootActorPath, Integer> processorIndices;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .processorIndices(new HashMap<>(getProcessorIndices()))
                            .build();
        }
    }
}
