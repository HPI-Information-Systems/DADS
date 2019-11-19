package de.hpi.msc.jschneider.actor.common;

import akka.actor.AbstractLoggingActor;
import akka.actor.Terminated;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;

public abstract class AbstractActor<TActorModel extends ActorModel, TActorControl extends ActorControl<TActorModel>> extends AbstractLoggingActor
{
    private final TActorModel model;
    private final TActorControl control;

    protected AbstractActor()
    {
        model = createModel();
        if (model == null)
        {
            throw new NullPointerException("Model must not be null!");
        }

        control = createControl(model);
        if (control == null)
        {
            throw new NullPointerException("Control must not be null!");
        }
    }

    protected abstract TActorModel createModel();

    protected abstract TActorControl createControl(TActorModel model);

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
