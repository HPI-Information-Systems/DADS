package de.hpi.msc.jschneider.protocol.common.eventDispatcher;

import akka.actor.AbstractLoggingActor;
import akka.actor.Props;
import de.hpi.msc.jschneider.protocol.common.control.ProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.common.model.ProtocolParticipantModel;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeProtocol;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;

public class EventDispatcher<TModel extends EventDispatcherModel, TControl extends EventDispatcherControl<TModel>> extends AbstractLoggingActor
{
    private TControl control;

    protected EventDispatcher(TControl control)
    {
        setControl(control);
    }

    public static <TModel extends EventDispatcherModel, TControl extends EventDispatcherControl<TModel>> Props props(TControl control)
    {
        return Props.create(EventDispatcher.class, () -> new EventDispatcher(control));
    }

    protected final TModel getModel()
    {
        return getControl().getModel();
    }

    protected final void setModel(TModel model)
    {
        if (model == null)
        {
            return;
        }

        initializeModel(model);
        getControl().setModel(model);
    }

    protected final TControl getControl()
    {
        return control;
    }

    protected final void setControl(TControl control)
    {
        if (control == null)
        {
            throw new NullPointerException();
        }

        this.control = control;
        initializeModel(this.control.getModel());
    }

    protected void initializeModel(TModel model)
    {
        if (MessageExchangeProtocol.isInitialized())
        {
            model.setMessageDispatcherProvider(message -> MessageExchangeProtocol.getLocalRootActor());
        }
        else
        {
            model.setMessageDispatcherProvider(MessageExchangeMessages.MessageExchangeMessage::getReceiver);
        }
    }

    @Override
    public Receive createReceive()
    {
        return getControl().complementReceiveBuilder(new ImprovedReceiveBuilder()).build();
    }
}
