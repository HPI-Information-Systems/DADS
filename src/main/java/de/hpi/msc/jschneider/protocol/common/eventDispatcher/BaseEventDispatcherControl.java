package de.hpi.msc.jschneider.protocol.common.eventDispatcher;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import lombok.val;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class BaseEventDispatcherControl<TModel extends EventDispatcherModel> implements EventDispatcherControl<TModel>
{
    private Logger log;

    protected final Logger getLog()
    {
        if (log == null)
        {
            log = LogManager.getLogger(getClass());
        }

        return log;
    }

    private TModel model;

    @Override
    public final TModel getModel()
    {
        return model;
    }

    @Override
    public final void setModel(TModel model)
    {
        if (model == null)
        {
            throw new NullPointerException();
        }

        this.model = model;
    }

    public BaseEventDispatcherControl(TModel model)
    {
        setModel(model);
    }

    @Override
    public ImprovedReceiveBuilder complementReceiveBuilder(ImprovedReceiveBuilder builder)
    {
        return builder.match(EventDispatcherMessages.SubscribeToEventMessage.class, this::onSubscribe)
                      .match(EventDispatcherMessages.UnsubscribeFromEventMessage.class, this::onUnsubscribe)
                      .match(MessageExchangeMessages.MessageExchangeMessage.class, this::onDispatchEvent)
                      .match(MessageExchangeMessages.BackPressureMessage.class, this::onBackPressure)
                      .matchAny(this::onAny);
    }

    protected void onSubscribe(EventDispatcherMessages.SubscribeToEventMessage message)
    {
        try
        {
            val subscribers = getModel().getEventSubscribers().get(message.getEventType());
            if (subscribers == null)
            {
                getLog().error(String.format("%1$s tried to subscribe to %2$s, which is not supported by this %3$s!",
                                             message.getSender(),
                                             message.getEventType().getName(),
                                             getClass().getName()));
                return;
            }

            subscribers.add(message.getSender());
        }
        finally
        {
            complete(message);
        }
    }

    protected void onUnsubscribe(EventDispatcherMessages.UnsubscribeFromEventMessage message)
    {
        try
        {
            val subscribers = getModel().getEventSubscribers().get(message.getEventType());
            if (subscribers == null)
            {
                getLog().error(String.format("%1$s tried to unsubscribe from %2$s, which is not supported by this %3$s!",
                                             message.getSender(),
                                             message.getEventType().getName(),
                                             getClass().getName()));
                return;
            }

            subscribers.remove(message.getSender());
        }
        finally
        {
            complete(message);
        }
    }

    protected void onDispatchEvent(MessageExchangeMessages.MessageExchangeMessage message)
    {
        try
        {
            val subscribers = getModel().getEventSubscribers().get(message.getClass());
            if (subscribers == null)
            {
                getLog().error(String.format("%1$s tried to dispatch %2$s, which is not supported by this %3$s!",
                                             message.getSender(),
                                             message.getClass().getName(),
                                             getClass().getName()));
                return;
            }

            for (val subscriber : subscribers)
            {
                message.setReceiver(subscriber);
                send(message);
            }
        }
        finally
        {
            complete(message);
        }
    }

    protected void onBackPressure(MessageExchangeMessages.BackPressureMessage message) throws InterruptedException
    {
        try
        {
            Thread.sleep(1000);
        }
        finally
        {
            complete(message);
        }
    }

    protected void onAny(Object message)
    {
        getLog().warn(String.format("%1$s received unmatched message of type %2$s!",
                                    getClass().getName(),
                                    message.getClass().getName()));
    }

    protected void complete(MessageExchangeMessages.MessageExchangeMessage message)
    {
        val completedMessage = MessageExchangeMessages.MessageCompletedMessage.builder()
                                                                              .sender(message.getReceiver())
                                                                              .receiver(message.getSender())
                                                                              .completedMessageId(message.getId())
                                                                              .build();

        send(completedMessage);
    }


    protected void send(MessageExchangeMessages.MessageExchangeMessage message)
    {
        if (message == null || message.getReceiver() == null || message.getReceiver() == ActorRef.noSender())
        {
            return;
        }

        val messageDispatcher = getModel().getMessageDispatcher(message);
        messageDispatcher.tell(message, message.getSender());
    }
}
