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
}
