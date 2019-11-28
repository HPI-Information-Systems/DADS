package de.hpi.msc.jschneider.protocol.common;

import akka.actor.AbstractLoggingActor;
import akka.actor.Props;
import de.hpi.msc.jschneider.protocol.common.control.ProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.common.model.ProtocolParticipantModel;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorRegistrationProtocol;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;

public class ProtocolParticipant<TModel extends ProtocolParticipantModel, TControl extends ProtocolParticipantControl<TModel>> extends AbstractLoggingActor
{
    private TControl control;

    protected ProtocolParticipant(TControl control)
    {
        setControl(control);
    }

    public static <TModel extends ProtocolParticipantModel, TControl extends ProtocolParticipantControl<TModel>> Props props(TControl control)
    {
        return Props.create(ProtocolParticipant.class, () -> new ProtocolParticipant(control));
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
        model.setSelfProvider(this::self);
        model.setSenderProvider(this::self);
        model.setProcessorProvider(ProcessorRegistrationProtocol::getProcessor);
        model.setWatchActorCallback(context()::watch);
        model.setUnwatchActorCallback(context()::unwatch);
        model.setChildFactory(context()::actorOf);
    }

    protected ImprovedReceiveBuilder defaultReceiveBuilder()
    {
        return new ImprovedReceiveBuilder().matchAny(getControl()::onAny);
    }

    @Override
    public Receive createReceive()
    {
        return getControl().complementReceiveBuilder(defaultReceiveBuilder()).build();
    }
}
