package de.hpi.msc.jschneider.actor.common;

import akka.actor.ActorRef;
import akka.actor.Terminated;
import de.hpi.msc.jschneider.actor.common.messageExchange.messageProxy.MessageProxyMessages;

public interface ActorControl<TActorModel extends ActorModel>
{
    TActorModel getModel();

    void setModel(TActorModel model);

    void send(Object message, ActorRef receiver);

    void send(CompletableMessage message);

    void complete(CompletableMessage message);

    void onBackPressure(MessageProxyMessages.BackPressureMessage message);

    void onTerminated(Terminated message);

    void onAny(Object message);
}
