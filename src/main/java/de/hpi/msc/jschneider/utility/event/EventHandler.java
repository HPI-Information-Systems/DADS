package de.hpi.msc.jschneider.utility.event;

@FunctionalInterface
public interface EventHandler<TEventArgument>
{
    void invoke(TEventArgument argument);
}
