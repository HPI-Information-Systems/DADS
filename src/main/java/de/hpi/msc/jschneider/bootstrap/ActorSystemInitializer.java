package de.hpi.msc.jschneider.bootstrap;

import akka.actor.ActorSystem;
import de.hpi.msc.jschneider.actor.common.messageReceiverProxy.MessageReceiverProxy;
import de.hpi.msc.jschneider.actor.common.messageSenderProxy.MessageSenderProxy;
import de.hpi.msc.jschneider.actor.common.reaper.Reaper;
import de.hpi.msc.jschneider.bootstrap.command.AbstractCommand;
import de.hpi.msc.jschneider.bootstrap.command.MasterCommand;
import de.hpi.msc.jschneider.bootstrap.command.SlaveCommand;
import de.hpi.msc.jschneider.bootstrap.configuration.ConfigurationFactory;
import lombok.val;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeoutException;

public class ActorSystemInitializer
{
    private static final Logger Log = LogManager.getLogger(ActorSystemInitializer.class);

    private static final String MASTER_ACTOR_SYSTEM_NAME = "MasterActorSystem";
    private static final String SLAVE_ACTOR_SYSTEM_NAME = "SlaveActorSystem";

    public static void runMaster(MasterCommand masterCommand) throws Exception
    {
        val actorSystem = initializeActorSystem(MASTER_ACTOR_SYSTEM_NAME, masterCommand);

        awaitTermination(actorSystem);
    }

    public static void runSlave(SlaveCommand slaveCommand) throws Exception
    {
        val actorSystem = initializeActorSystem(SLAVE_ACTOR_SYSTEM_NAME, slaveCommand);

        awaitTermination(actorSystem);
    }

    private static ActorSystem initializeActorSystem(String name, AbstractCommand command) throws Exception
    {
        val configuration = ConfigurationFactory.createRemoteConfiguration(command.getHost(), command.getPort());
        val actorSystem = ActorSystem.create(name, configuration);

        MessageSenderProxy.initializePool(actorSystem, command.getNumberOfWorkers());
        MessageReceiverProxy.initializePool(actorSystem, command.getNumberOfWorkers());
        Reaper.getLocalActor(Reaper.initializeSingleton(actorSystem));

        return actorSystem;
    }

    private static void awaitTermination(ActorSystem actorSystem)
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
