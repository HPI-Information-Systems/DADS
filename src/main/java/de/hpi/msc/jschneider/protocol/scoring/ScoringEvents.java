package de.hpi.msc.jschneider.protocol.scoring;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

import java.time.LocalDateTime;

public class ScoringEvents
{
    @NoArgsConstructor @SuperBuilder @Getter
    public static class ReadyForTerminationEvent extends MessageExchangeMessages.RedirectableMessage
    {
        private static final long serialVersionUID = -2548952160769122196L;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .forwarder(getReceiver())
                            .build();
        }
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class PathScoringCompletedEvent extends MessageExchangeMessages.RedirectableMessage
    {
        private static final long serialVersionUID = 105573551477837831L;
        @NonNull
        private LocalDateTime startTime;
        @NonNull
        private LocalDateTime endTime;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .forwarder(getReceiver())
                            .startTime(getStartTime())
                            .endTime(getEndTime())
                            .build();
        }
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class PathScoreNormalizationCompletedEvent extends MessageExchangeMessages.RedirectableMessage
    {
        private static final long serialVersionUID = 2400245890773520483L;
        @NonNull
        private LocalDateTime startTime;
        @NonNull
        private LocalDateTime endTime;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .forwarder(getReceiver())
                            .startTime(getStartTime())
                            .endTime(getEndTime())
                            .build();
        }
    }
}
