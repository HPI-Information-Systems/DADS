package de.hpi.msc.jschneider.utility.event;

public interface Event<TEventArgument>
{
    void subscribe(EventHandler<TEventArgument> handler);

    void unsubscribe(EventHandler<TEventArgument> handler);
}
