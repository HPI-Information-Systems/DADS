package de.hpi.msc.jschneider.protocol.messageExchange.messageDispatcher;

import akka.actor.ActorRef;
import akka.actor.RootActorPath;
import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorId;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

@SuperBuilder
public class MessageDispatcherModel extends AbstractProtocolParticipantModel
{
    @NonNull @Getter @Builder.Default
    private Queue<MessageExchangeMessages.MessageExchangeMessage> undeliveredMessages = new LinkedList<>();
    @NonNull @Getter
    private final Map<ProcessorId, ActorRef> messageProxies = new HashMap<>();
}
