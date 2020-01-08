package de.hpi.msc.jschneider.protocol.dimensionReduction;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import de.hpi.msc.jschneider.utility.dataTransfer.DataTransferMessages;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

public class DimensionReductionMessages
{
    @NoArgsConstructor @SuperBuilder @Getter
    public static class InitializePrincipalComponentsTransferMessage extends DataTransferMessages.InitializeDataTransferMessage
    {
        private static final long serialVersionUID = -6259775947358143387L;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .operationId(getOperationId())
                            .build();
        }
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class InitializeRotationTransferMessage extends DataTransferMessages.InitializeDataTransferMessage
    {
        private static final long serialVersionUID = -5004178233413687871L;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .operationId(getOperationId())
                            .build();
        }
    }
}
