package de.hpi.msc.jschneider.actor.common;

import akka.actor.AbstractLoggingActor;
import akka.actor.Terminated;
import de.hpi.msc.jschneider.actor.utility.ImprovedReceiveBuilder;

public abstract class AbstractActor<TActorModel extends ActorModel, TActorControl extends ActorControl<TActorModel>> extends AbstractLoggingActor
{
    private TActorModel model;
    private TActorControl control;

    protected final void setModel(TActorModel model)
    {
        this.model = model;
    }

    protected final void setControl(TActorControl control)
    {
        this.control = control;
    }

    protected final TActorModel model()
    {
        return model;
    }

    protected final TActorControl control()
    {
        return control;
    }

    protected ImprovedReceiveBuilder defaultReceiveBuilder()
    {
        return new ImprovedReceiveBuilder().match(Terminated.class, control()::onTerminated)
                                           .matchAny(control()::onAny);
    }
}
