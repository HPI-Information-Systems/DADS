package de.hpi.msc.jschneider.protocol.messageExchange.messageDispatcher;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.common.CommonMessages;
import de.hpi.msc.jschneider.protocol.common.ProtocolParticipant;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import de.hpi.msc.jschneider.protocol.messageExchange.messageProxy.MessageProxyControl;
import de.hpi.msc.jschneider.protocol.messageExchange.messageProxy.MessageProxyModel;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorId;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorRegistrationEvents;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import lombok.val;
import lombok.var;

import java.util.Optional;

public class MessageDispatcherControl extends AbstractProtocolParticipantControl<MessageDispatcherModel>
{
    public MessageDispatcherControl(MessageDispatcherModel model)
    {
        super(model);
    }

    @Override
    public ImprovedReceiveBuilder complementReceiveBuilder(ImprovedReceiveBuilder builder)
    {
        return builder.match(CommonMessages.SetUpProtocolMessage.class, this::onSetUp)
                      .match(ProcessorRegistrationEvents.RegistrationAcknowledgedEvent.class, this::onRegistrationAcknowledged)
                      .match(ProcessorRegistrationEvents.ProcessorJoinedEvent.class, this::onProcessorJoined)
                      .match(MessageExchangeMessages.MessageExchangeMessage.class, this::onMessage);
    }

    private void onSetUp(CommonMessages.SetUpProtocolMessage message)
    {
        subscribeToLocalEvent(ProtocolType.ProcessorRegistration, ProcessorRegistrationEvents.RegistrationAcknowledgedEvent.class);
        subscribeToLocalEvent(ProtocolType.ProcessorRegistration, ProcessorRegistrationEvents.ProcessorJoinedEvent.class);
    }

    private void onRegistrationAcknowledged(ProcessorRegistrationEvents.RegistrationAcknowledgedEvent message)
    {
        if (!message.getReceiver().path().equals(getModel().getSelf().path()))
        {
            onMessage(message);
            return;
        }

        // we do NOT need to complete the message, because this message is not sent via a message proxy and therefore
        // is not stored in one of the queues
        dequeueUndeliveredMessages();
    }

    private void onProcessorJoined(ProcessorRegistrationEvents.ProcessorJoinedEvent message)
    {
        if (!message.getReceiver().path().equals(getModel().getSelf().path()))
        {
            onMessage(message);
            return;
        }

        // we do NOT need to complete the message, because this message is not sent via a message proxy and therefore
        // is not stored in one of the queues
        dequeueUndeliveredMessages();
    }

    private void dequeueUndeliveredMessages()
    {
        val previousQueueSize = getModel().getUndeliveredMessages().size();
        if (previousQueueSize > 0)
        {
            getLog().info(String.format("Trying to dequeue %1$d undelivered message(s).", previousQueueSize));
        }

        while (!getModel().getUndeliveredMessages().isEmpty())
        {
            val message = getModel().getUndeliveredMessages().peek();
            if (message == null)
            {
                getModel().getUndeliveredMessages().poll();
                continue;
            }

            if (!tryDeliver(message))
            {
                getLog().info(String.format("Unable to deliver message (type = %1$s) from %2$s to %3$s.",
                                            message.getClass().getName(),
                                            message.getSender().path(),
                                            message.getReceiver().path()));
                break;
            }

            getModel().getUndeliveredMessages().poll();
        }

        val queueSizeDifference = getModel().getUndeliveredMessages().size() - previousQueueSize;
        if (queueSizeDifference > 0)
        {
            getLog().info(String.format("Undelivered message queue size just shrunk by %1$d and is now %2$d.",
                                        queueSizeDifference,
                                        getModel().getUndeliveredMessages().size()));
        }
    }

    private void onMessage(MessageExchangeMessages.MessageExchangeMessage message)
    {
        dequeueUndeliveredMessages();

        if (tryDeliver(message))
        {
            return;
        }

        getModel().getUndeliveredMessages().add(message);
        getLog().info(String.format("Undelivered message queue just grew to %1$d.", getModel().getUndeliveredMessages().size()));
    }

    private boolean tryDeliver(MessageExchangeMessages.MessageExchangeMessage message)
    {
        val proxy = getMessageProxy(message);
        if (!proxy.isPresent())
        {
            return false;
        }

        proxy.get().tell(message, message.getSender());
        return true;
    }

    private Optional<ActorRef> getMessageProxy(MessageExchangeMessages.MessageExchangeMessage message)
    {
        var remoteProcessorId = ProcessorId.of(message.getReceiver());
        if (remoteProcessorId.equals(ProcessorId.of(getModel().getSelf())))
        {
            remoteProcessorId = ProcessorId.of(message.getSender());

            if (message instanceof MessageExchangeMessages.RedirectableMessage)
            {
                val forwarder = ((MessageExchangeMessages.RedirectableMessage) message).getForwarder();
                if (forwarder != null && !forwarder.equals(ActorRef.noSender()))
                {
                    remoteProcessorId = ProcessorId.of(forwarder);
                }
            }
        }

        return getOrCreateMessageProxy(remoteProcessorId);
    }

    private Optional<ActorRef> getOrCreateMessageProxy(ProcessorId processorId)
    {
        var messageProxy = Optional.ofNullable(getModel().getMessageProxies().get(processorId));
        if (!messageProxy.isPresent())
        {
            messageProxy = tryCreateMessageProxy(processorId);
            messageProxy.ifPresent(actorRef -> getModel().getMessageProxies().put(processorId, actorRef));
        }

        return messageProxy;
    }

    private Optional<ActorRef> tryCreateMessageProxy(ProcessorId processorId)
    {
        val remoteMessageDispatcher = getProtocol(processorId, ProtocolType.MessageExchange);
        if (!remoteMessageDispatcher.isPresent())
        {
            getLog().error(String.format("Unable to get the MessageExchange root actor for (remote) actor system at \"%1$s\"!",
                                         processorId));

            return Optional.empty();
        }

        val model = MessageProxyModel.builder()
                                     .remoteMessageDispatcher(remoteMessageDispatcher.get().getRootActor())
                                     .build();

        val control = new MessageProxyControl(model);

        return trySpawnChild(ProtocolParticipant.props(control), "MessageProxy");
    }
}
