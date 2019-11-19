package de.hpi.msc.jschneider.actor.common;

import akka.actor.ActorRef;
import akka.actor.Props;
import lombok.Getter;
import lombok.Setter;

import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.assertj.core.api.Fail.fail;


public class MockPostbox implements Postbox
{
    @Setter
    private Callable<ActorRef> senderFactory = ActorRef::noSender;
    @Getter
    private int senderFactoryCalls = 0;

    @Setter
    private Callable<ActorRef> selfFactory = ActorRef::noSender;
    @Getter
    private int selfFactoryCalls = 0;

    @Setter
    private Function<Props, ActorRef> childFactory = props -> ActorRef.noSender();
    @Getter
    private int childFactoryCalls = 0;

    @Setter
    private Consumer<ActorRef> watchCallback = actor ->
    {
    };
    @Getter
    private int watchCallbackCalls = 0;

    @Setter
    private Consumer<ActorRef> unwatchCallback = actor ->
    {
    };
    @Getter
    private int unwatchCallbackCalls = 0;

    @Override
    public ActorRef sender()
    {
        senderFactoryCalls++;
        try
        {
            return senderFactory.call();
        }
        catch (Exception exception)
        {
            fail("Unhandled exception while calling \"getSender\"", exception);
            return ActorRef.noSender();
        }
    }

    @Override
    public ActorRef self()
    {
        selfFactoryCalls++;
        try
        {
            return selfFactory.call();
        }
        catch (Exception exception)
        {
            fail("Unhandled exception while calling \"getSelf\"", exception);
            return ActorRef.noSender();
        }
    }

    @Override
    public ActorRef spawnChild(Props props)
    {
        childFactoryCalls++;
        try
        {
            return childFactory.apply(props);
        }
        catch (Exception exception)
        {
            fail("Unhandled exception while calling \"spawnChild\"", exception);
            return ActorRef.noSender();
        }
    }

    @Override
    public void watch(ActorRef actor)
    {
        watchCallbackCalls++;
        try
        {
            watchCallback.accept(actor);
        }
        catch (Exception exception)
        {
            fail("Unhandled exception while calling \"watch\"", exception);
        }
    }

    @Override
    public void unwatch(ActorRef actor)
    {
        unwatchCallbackCalls++;
        try
        {
            unwatchCallback.accept(actor);
        }
        catch (Exception exception)
        {
            fail("Unhandled exception while calling \"unwatch\"", exception);
        }
    }
}
