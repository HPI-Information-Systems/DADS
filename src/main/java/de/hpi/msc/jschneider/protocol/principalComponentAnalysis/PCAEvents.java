package de.hpi.msc.jschneider.protocol.principalComponentAnalysis;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.ojalgo.matrix.decomposition.SingularValue;

public class PCAEvents
{
    @NoArgsConstructor @SuperBuilder @Getter
    public static class SingularValueDecompositionCreatedEvent extends MessageExchangeMessages.RedirectableMessage
    {
        private static final long serialVersionUID = 2609090276705058071L;
        private SingularValue<Double> svd;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .svd(getSvd())
                            .build();
        }
    }
}
