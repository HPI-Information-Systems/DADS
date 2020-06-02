package de.hpi.msc.jschneider.protocol.messageExchange.messageDispatcher;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.common.CommonMessages;
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
                      .match(MessageExchangeMessages.IntroduceMessageProxyMessage.class, this::onIntroduceMessageProxy)
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
        for (val processor : getModel().getProcessors())
        {
            introduceMessageProxy(processor.getId());
        }

        dequeueUndeliveredMessages();
    }

    private void introduceMessageProxy(ProcessorId remoteProcessor)
    {
        val remoteProtocol = getProtocol(remoteProcessor, ProtocolType.MessageExchange);
        if (!remoteProtocol.isPresent())
        {
            getLog().error("Unable to get MessageExchange protocol of {}!", remoteProcessor);
            return;
        }

        val proxy = getOrCreateMessageProxy(remoteProcessor);
        if (!proxy.isPresent())
        {
            getLog().error("Unable to get MessageProxy for processor {}!", remoteProcessor);
            return;
        }

        val receiver = remoteProtocol.get().getRootActor();
        receiver.tell(MessageExchangeMessages.IntroduceMessageProxyMessage.builder()
                                                                          .sender(getModel().getSelf())
                                                                          .receiver(receiver)
                                                                          .messageProxy(proxy.get())
                                                                          .build(),
                      getModel().getSelf());
    }

    private void onIntroduceMessageProxy(MessageExchangeMessages.IntroduceMessageProxyMessage message)
    {
        getModel().getRemoteMessageProxies().put(ProcessorId.of(message.getSender()), message.getMessageProxy());

        val proxy = getOrCreateMessageProxy(ProcessorId.of(message.getSender()));
        if (!proxy.isPresent())
        {
            getLog().error("Unable to get MessageProxy for processor {}!", ProcessorId.of(message.getSender()));
            return;
        }

        proxy.get().tell(MessageExchangeMessages.UpdateRemoteMessageReceiverMessage.builder()
                                                                                   .remoteMessageReceiver(message.getMessageProxy())
                                                                                   .build(),
                         getModel().getSelf());
    }

    private void onProcessorJoined(ProcessorRegistrationEvents.ProcessorJoinedEvent message)
    {
        if (!message.getReceiver().path().equals(getModel().getSelf().path()))
        {
            onMessage(message);
            return;
        }

        // we do NOT need to complete this message, because it is not sent via a message proxy and therefore
        // is not stored in one of the queues
        dequeueUndeliveredMessages();
    }

    private void dequeueUndeliveredMessages()
    {
        val previousQueueSize = getModel().getUndeliveredMessages().size();
        if (previousQueueSize > 0)
        {
            getLog().info("Trying to dequeue {} undelivered message(s).", previousQueueSize);
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
                getLog().info("Unable to deliver message (type = {}) from {} to {}.",
                              message.getClass().getName(),
                              message.getSender().path(),
                              message.getReceiver().path());
                break;
            }

            getModel().getUndeliveredMessages().poll();
        }

        val queueSizeDifference = getModel().getUndeliveredMessages().size() - previousQueueSize;
        if (queueSizeDifference > 0)
        {
            getLog().info("Undelivered message queue size just shrunk by {} and is now {}.",
                          queueSizeDifference,
                          getModel().getUndeliveredMessages().size());
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
        getLog().info("Undelivered message queue just grew to {}.", getModel().getUndeliveredMessages().size());
    }

    private boolean tryDeliver(MessageExchangeMessages.MessageExchangeMessage message)
    {
        val receiverProcessor = ProcessorId.of(message.getReceiver());

        if ((message instanceof MessageExchangeMessages.MessageCompletedMessage)
            && !receiverProcessor.equals(ProcessorId.of(getModel().getSelf())))
        {
            // deliver acknowledgement directly to remote MessageProxy
            return tryDeliver((MessageExchangeMessages.MessageCompletedMessage) message);
        }

        val proxy = getMessageProxy(message);
        if (!proxy.isPresent())
        {
            return false;
        }

        proxy.get().tell(message, message.getSender());
        return true;
    }

    private boolean tryDeliver(MessageExchangeMessages.MessageCompletedMessage message)
    {
        val receiverProcessor = ProcessorId.of(message.getReceiver());
        val receiverMessageProxy = getModel().getRemoteMessageProxies().get(receiverProcessor);

        if (receiverMessageProxy != null)
        {
            receiverMessageProxy.tell(message, getModel().getSelf());
            return true;
        }

        val remoteProtocol = getProtocol(receiverProcessor, ProtocolType.MessageExchange);
        if (!remoteProtocol.isPresent())
        {
            getLog().error("Unable to get MessageExchange protocol for {}!", receiverProcessor);
            return false;
        }

        remoteProtocol.get().getRootActor().tell(message, getModel().getSelf());
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
        var messageProxy = Optional.ofNullable(getModel().getLocalMessageProxies().get(processorId));
        if (!messageProxy.isPresent())
        {
            messageProxy = tryCreateMessageProxy(processorId);
            messageProxy.ifPresent(actorRef -> getModel().getLocalMessageProxies().put(processorId, actorRef));
        }

        return messageProxy;
    }

    private Optional<ActorRef> tryCreateMessageProxy(ProcessorId processorId)
    {
        val remoteMessageDispatcher = getProtocol(processorId, ProtocolType.MessageExchange);
        if (!remoteMessageDispatcher.isPresent())
        {
            getLog().error("Unable to get the MessageExchange root actor for (remote) actor system at \"{}\"!",
                           processorId);

            return Optional.empty();
        }

        val model = MessageProxyModel.builder()
                                     .remoteMessageReceiver(remoteMessageDispatcher.get().getRootActor())
                                     .schedulerProvider(getModel().getSchedulerProvider())
                                     .dispatcherProvider(getModel().getDispatcherProvider())
                                     .build();

        val control = new MessageProxyControl(model);

        return trySpawnChild(control, "MessageProxy");
    }
}
