package de.hpi.msc.jschneider.actor.common;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.LoggingAdapter;

public interface Postbox
{
    ActorRef sender();

    ActorRef self();

    ActorRef spawnChild(Props props);

    void watch(ActorRef actor);

    void unwatch(ActorRef actor);
}
