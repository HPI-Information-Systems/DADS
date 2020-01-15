package de.hpi.msc.jschneider.protocol.common.eventDispatcher;

import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import lombok.val;

public class BaseEventDispatcherControl<TModel extends EventDispatcherModel> extends AbstractProtocolParticipantControl<TModel> implements EventDispatcherControl<TModel>
{
    public BaseEventDispatcherControl(TModel model)
    {
        super(model);
    }

    @Override
    public ImprovedReceiveBuilder complementReceiveBuilder(ImprovedReceiveBuilder builder)
    {
        return super.complementReceiveBuilder(builder)
                    .match(EventDispatcherMessages.SubscribeToEventMessage.class, this::onSubscribe)
                    .match(EventDispatcherMessages.UnsubscribeFromEventMessage.class, this::onUnsubscribe)
                    .match(MessageExchangeMessages.RedirectableMessage.class, this::onDispatchEvent)
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

            if (subscribers.add(message.getSender()))
            {
                getLog().info(String.format("%1$s just subscribed to %2$s.", message.getSender().path(), message.getEventType().getName()));
            }
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

            if (subscribers.remove(message.getSender()))
            {
                getLog().info(String.format("%1$s just unsubscribed from %2$s.", message.getSender().path(), message.getEventType().getName()));
            }
        }
        finally
        {
            complete(message);
        }
    }

    protected void onDispatchEvent(MessageExchangeMessages.RedirectableMessage message)
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

            getLog().info(String.format("Dispatching %1$s to %2$d subscribers.", message.getClass().getName(), subscribers.size()));

            for (val subscriber : subscribers)
            {
                send(message.redirectTo(subscriber));
            }
        }
        finally
        {
            complete(message);
        }
    }
}
