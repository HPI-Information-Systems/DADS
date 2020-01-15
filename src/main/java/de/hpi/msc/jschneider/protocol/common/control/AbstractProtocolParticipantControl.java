package de.hpi.msc.jschneider.protocol.common.control;

import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.RootActorPath;
import de.hpi.msc.jschneider.protocol.common.Protocol;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.eventDispatcher.EventDispatcherMessages;
import de.hpi.msc.jschneider.protocol.common.model.ProtocolParticipantModel;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import de.hpi.msc.jschneider.protocol.processorRegistration.Processor;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import de.hpi.msc.jschneider.utility.dataTransfer.DataTransferManager;
import de.hpi.msc.jschneider.utility.dataTransfer.DataTransferMessages;
import lombok.val;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;
import java.util.function.Function;

public abstract class AbstractProtocolParticipantControl<TModel extends ProtocolParticipantModel> implements ProtocolParticipantControl<TModel>
{
    private Logger log;
    private TModel model;

    protected AbstractProtocolParticipantControl(TModel model)
    {
        setModel(model);
        model.setDataTransferManager(new DataTransferManager(this));
    }

    protected final Logger getLog()
    {
        if (log == null)
        {
            log = LogManager.getLogger(getClass());
        }

        return log;
    }

    @Override
    public void preStart()
    {

    }

    @Override
    public void postStop()
    {

    }

    @Override
    public ImprovedReceiveBuilder complementReceiveBuilder(ImprovedReceiveBuilder builder)
    {
        return builder.match(DataTransferMessages.RequestNextDataPartMessage.class, getModel().getDataTransferManager()::onRequestNextPart)
                      .match(DataTransferMessages.DataPartMessage.class, getModel().getDataTransferManager()::onPart)
                      .match(MessageExchangeMessages.BackPressureMessage.class, this::onBackPressure);
    }

    protected void onBackPressure(MessageExchangeMessages.BackPressureMessage message)
    {
        try
        {
            Thread.sleep(1000);
        }
        catch (InterruptedException interruptedException)
        {
            getLog().warn("Error while performing back pressure!", interruptedException);
        }
        finally
        {
            complete(message);
        }
    }

    @Override
    public final Optional<Protocol> getLocalProtocol(ProtocolType protocolType)
    {
        return getProtocol(getModel().getSelf().path().root(), protocolType);
    }

    @Override
    public final Optional<Protocol> getMasterProtocol(ProtocolType protocolType)
    {
        val processor = getModel().getMasterProcessor();
        if (!processor.isPresent())
        {
            return Optional.empty();
        }

        return getProtocol(processor, protocolType);
    }

    @Override
    public final Optional<Protocol> getProtocol(RootActorPath actorSystem, ProtocolType protocolType)
    {
        val processor = getModel().getProcessor(actorSystem);
        if (!processor.isPresent())
        {
            return Optional.empty();
        }

        return getProtocol(processor, protocolType);
    }

    private Optional<Protocol> getProtocol(Optional<Processor> processor, ProtocolType protocolType)
    {
        if (!processor.isPresent())
        {
            return Optional.empty();
        }

        for (val protocol : processor.get().getProtocols())
        {
            if (protocol.getType() == protocolType)
            {
                return Optional.of(protocol);
            }
        }

        return Optional.empty();
    }

    @Override
    public final TModel getModel()
    {
        return model;
    }

    @Override
    public final void setModel(TModel model)
    {
        if (model == null)
        {
            throw new NullPointerException();
        }

        this.model = model;
    }

    @Override
    public final boolean tryWatch(ActorRef subject)
    {
        if (subject == null || subject == ActorRef.noSender())
        {
            return false;
        }

        try
        {
            getModel().getWatchActorCallback().accept(subject);
            return getModel().getWatchedActors().add(subject);
        }
        catch (Exception exception)
        {
            getLog().error("Unable to watch actor!", exception);
            return false;
        }
    }

    @Override
    public final boolean tryUnwatch(ActorRef subject)
    {
        if (subject == null || subject == ActorRef.noSender())
        {
            return false;
        }

        try
        {
            getModel().getUnwatchActorCallback().accept(subject);
            return getModel().getWatchedActors().remove(subject);
        }
        catch (Exception exception)
        {
            getLog().error("Unable to unwatch actor!", exception);
            return false;
        }
    }

    @Override
    public final Optional<ActorRef> trySpawnChild(Props props, String name)
    {
        try
        {
            val child = getModel().getChildFactory().create(props, name);
            if (!getModel().getChildActors().add(child))
            {
                child.tell(PoisonPill.getInstance(), getModel().getSelf());
                getLog().error("Unable to add newly created child! This should never happen!");
                return Optional.empty();
            }

            if (!tryWatch(child))
            {
                child.tell(PoisonPill.getInstance(), getModel().getSelf());
                getLog().error("Unable to watch newly created child! This should never happen!");
                return Optional.empty();
            }

            return Optional.of(child);
        }
        catch (Exception exception)
        {
            getLog().error("Unable to create a new child!", exception);
            return Optional.empty();
        }
    }

    @Override
    public void onAny(Object message)
    {
        getLog().warn(String.format("%1$s received unmatched message of type %2$s!",
                                    getClass().getName(),
                                    message.getClass().getName()));
    }

    @Override
    public void subscribeToLocalEvent(ProtocolType protocolType, Class<? extends MessageExchangeMessages.RedirectableMessage> eventType)
    {
        subscribeToEvent(getLocalProtocol(protocolType), eventType);
    }

    @Override
    public void subscribeToMasterEvent(ProtocolType protocolType, Class<? extends MessageExchangeMessages.RedirectableMessage> eventType)
    {
        subscribeToEvent(getMasterProtocol(protocolType), eventType);
    }

    @Override
    public void subscribeToEvent(RootActorPath actorSystem, ProtocolType protocolType, Class<? extends MessageExchangeMessages.RedirectableMessage> eventType)
    {
        subscribeToEvent(getProtocol(actorSystem, protocolType), eventType);
    }

    private void subscribeToEvent(Optional<Protocol> protocol, Class<? extends MessageExchangeMessages.RedirectableMessage> eventType)
    {
        protocol.ifPresent(actualProtocol ->
                           {
                               send(EventDispatcherMessages.SubscribeToEventMessage.builder()
                                                                                   .sender(getModel().getSelf())
                                                                                   .receiver(actualProtocol.getEventDispatcher())
                                                                                   .eventType(eventType)
                                                                                   .build());
                           });
    }

    @Override
    public void forward(MessageExchangeMessages.RedirectableMessage message, ActorRef receiver)
    {
        try
        {
            send(message.redirectTo(receiver));
        }
        finally
        {
            complete(message);
        }
    }

    @Override
    public void send(Object message, ActorRef receiver)
    {
        if (message == null || receiver == null || receiver == ActorRef.noSender())
        {
            return;
        }

        if (message instanceof MessageExchangeMessages.MessageExchangeMessage)
        {
            send((MessageExchangeMessages.MessageExchangeMessage) message);
            return;
        }

        receiver.tell(message, getModel().getSelf());
    }

    @Override
    public void send(MessageExchangeMessages.MessageExchangeMessage message)
    {
        if (message == null || message.getReceiver() == ActorRef.noSender())
        {
            return;
        }

        val protocol = getLocalProtocol(ProtocolType.MessageExchange);
        if (protocol.isPresent())
        {
            protocol.get().getRootActor().tell(message, message.getSender());
        }
        else
        {
            message.getReceiver().tell(message, message.getSender());
        }
    }

    @Override
    public boolean trySendEvent(ProtocolType protocolType, Function<ActorRef, MessageExchangeMessages.RedirectableMessage> eventFactory)
    {
        try
        {
            val protocol = getLocalProtocol(protocolType);
            protocol.ifPresent(p -> send(eventFactory.apply(p.getEventDispatcher())));

            return protocol.isPresent();
        }
        catch (Exception exception)
        {
            getLog().error("Unable to send event!", exception);
            return false;
        }
    }

    @Override
    public void complete(MessageExchangeMessages.MessageExchangeMessage message)
    {
        if (message == null)
        {
            return;
        }

        getLocalProtocol(ProtocolType.MessageExchange).ifPresent(protocol ->
                                                                 {
                                                                     send(MessageExchangeMessages.MessageCompletedMessage.builder()
                                                                                                                         .sender(message.getReceiver())
                                                                                                                         .receiver(message.getSender())
                                                                                                                         .completedMessageId(message.getId())
                                                                                                                         .build());
                                                                 });
    }
}