package de.hpi.msc.jschneider.actor.common;

import akka.actor.Terminated;

public interface ActorControl<TActorModel extends ActorModel>
{
    TActorModel getModel();

    void setModel(TActorModel model);

    void send(Message message);

    void onTerminated(Terminated message);

    void onAny(Object message);
}
