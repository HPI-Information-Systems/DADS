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
                getLog().error("{} tried to subscribe to {}, which is not supported by this {}!",
                               message.getSender(),
                               message.getEventType().getName(),
                               getClass().getName());
                return;
            }

            if (subscribers.add(message.getSender()))
            {
                getLog().debug("{} just subscribed to {}.", message.getSender().path(), message.getEventType().getName());
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
                getLog().error("{} tried to unsubscribe from {}, which is not supported by this {}!",
                               message.getSender(),
                               message.getEventType().getName(),
                               getClass().getName());
                return;
            }

            if (subscribers.remove(message.getSender()))
            {
                getLog().debug("{} just unsubscribed from {}.", message.getSender().path(), message.getEventType().getName());
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
                getLog().error("{} tried to dispatch {}, which is not supported by this {}!",
                               message.getSender(),
                               message.getClass().getName(),
                               getClass().getName());
                return;
            }

            getLog().debug("Dispatching {} to {} subscribers.", message.getClass().getName(), subscribers.size());

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
