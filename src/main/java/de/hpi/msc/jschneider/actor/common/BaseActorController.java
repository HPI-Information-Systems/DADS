package de.hpi.msc.jschneider.actor.common;

import akka.actor.ActorRef;
import akka.actor.Props;
import lombok.val;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.Set;

public abstract class BaseActorController
{
    private Logger log;
    private final Postbox owner;
    protected final Set<ActorRef> watchedActors = new HashSet<>();
    protected final Set<ActorRef> children = new HashSet<>();

    protected BaseActorController(Postbox owner)
    {
        if (owner == null)
        {
            throw new NullPointerException();
        }

        this.owner = owner;
    }

    protected ActorRef sender()
    {
        return owner.sender();
    }

    protected ActorRef self()
    {
        return owner.self();
    }

    protected Logger log()
    {
        if (log == null)
        {
            log = LogManager.getLogger(getClass());
        }

        return log;
    }

    protected ActorRef spawnChild(Props props)
    {
        val child = owner.spawnChild(props);
        children.add(child);
        watch(child);

        return child;
    }

    protected boolean watch(ActorRef actor)
    {
        owner.watch(actor);
        return watchedActors.add(actor);
    }

    protected boolean unwatch(ActorRef actor)
    {
        owner.unwatch(actor);
        return watchedActors.remove(actor);
    }

    public boolean hasChildren()
    {
        return !children.isEmpty();
    }

    public boolean hasWatchedActors()
    {
        return !watchedActors.isEmpty();
    }

    protected boolean senderTerminated()
    {
        return unwatch(sender()) && children.remove(sender());
    }
}
