package de.hpi.msc.jschneider.bootstrap;

import akka.actor.ActorSystem;
import de.hpi.msc.jschneider.actor.common.messageReceiverProxy.MessageReceiverProxy;
import de.hpi.msc.jschneider.actor.common.messageSenderProxy.MessageSenderProxy;
import de.hpi.msc.jschneider.actor.common.reaper.Reaper;
import de.hpi.msc.jschneider.bootstrap.command.MasterCommand;
import de.hpi.msc.jschneider.bootstrap.command.SlaveCommand;
import de.hpi.msc.jschneider.bootstrap.configuration.ConfigurationFactory;
import lombok.val;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import java.io.FileNotFoundException;
import java.util.concurrent.TimeoutException;

public class ActorSystemInitializer
{
    private static final Logger Log = LogManager.getLogger(ActorSystemInitializer.class);

    private static final String MASTER_ACTOR_SYSTEM_NAME = "MasterActorSystem";
    private static final String SLAVE_ACTOR_SYSTEM_NAME = "SlaveActorSystem";

    public static void runMaster(final MasterCommand masterCommand) throws FileNotFoundException
    {
        val actorSystem = initializeActorSystem(MASTER_ACTOR_SYSTEM_NAME, masterCommand.getHost(), masterCommand.getPort());

        awaitTermination(actorSystem);
    }

    public static void runSlave(final SlaveCommand slaveCommand) throws FileNotFoundException
    {
        val actorSystem = initializeActorSystem(SLAVE_ACTOR_SYSTEM_NAME, slaveCommand.getHost(), slaveCommand.getPort());

        awaitTermination(actorSystem);
    }

    private static ActorSystem initializeActorSystem(final String name, final String host, final int port) throws FileNotFoundException
    {
        val configuration = ConfigurationFactory.createRemoteConfiguration(host, port);
        val actorSystem = ActorSystem.create(name, configuration);

        MessageSenderProxy.globalInstance(MessageSenderProxy.createIn(actorSystem));
        MessageReceiverProxy.globalInstance(MessageReceiverProxy.createIn(actorSystem));
        Reaper.globalInstance(Reaper.createIn(actorSystem));

        return actorSystem;
    }

    private static void awaitTermination(final ActorSystem actorSystem)
    {
        try
        {
            Await.ready(actorSystem.whenTerminated(), Duration.Inf());
        }
        catch (TimeoutException | InterruptedException exception)
        {
            exception.printStackTrace();
            System.exit(1);
        }

        Log.info("ActorSystem terminated gracefully.");
    }
}
