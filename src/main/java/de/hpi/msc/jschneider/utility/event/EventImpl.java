package de.hpi.msc.jschneider.utility.event;

import lombok.val;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.Set;


public class EventImpl<TArgument> implements Event<TArgument>
{
    private static final Logger Log = LogManager.getLogger(EventImpl.class);

    private final Set<EventHandler<TArgument>> handlers = new HashSet<>();

    @Override
    public void subscribe(EventHandler<TArgument> handler)
    {
        assert handler != null : "EventHandlers must not be null!";

        handlers.add(handler);
    }

    @Override
    public void unsubscribe(EventHandler<TArgument> handler)
    {
        handlers.remove(handler);
    }

    public void invoke(TArgument argument)
    {
        for (val handler : handlers)
        {
            try
            {
                handler.invoke(argument);
            }
            catch (Exception exception)
            {
                Log.error("Exception while invoking event handler!", exception);
            }
        }
    }
}
