package de.hpi.msc.jschneider.protocol.principalComponentAnalysis;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import org.ojalgo.matrix.store.MatrixStore;

public class PCAEvents
{
    @NoArgsConstructor @SuperBuilder @Getter
    public static class PrincipalComponentsCreatedEvent extends MessageExchangeMessages.RedirectableMessage
    {
        private static final long serialVersionUID = 2609090276705058071L;
        @NonNull
        private MatrixStore<Double> principalComponents;
        @NonNull
        private MatrixStore<Double> rotation;
        @NonNull
        private MatrixStore<Double> columnMeans;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .principalComponents(getPrincipalComponents())
                            .rotation(getRotation())
                            .columnMeans(getColumnMeans())
                            .build();
        }
    }
}
