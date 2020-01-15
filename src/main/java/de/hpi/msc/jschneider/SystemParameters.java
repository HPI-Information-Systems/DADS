package de.hpi.msc.jschneider;

import com.typesafe.config.Config;
import de.hpi.msc.jschneider.bootstrap.command.AbstractCommand;
import lombok.val;

import java.nio.file.Path;

public class SystemParameters
{
    private static AbstractCommand command;
    private static Config configuration;
    private static long maximumMemory;

    public static void initialize(AbstractCommand command, Config configuration)
    {
        SystemParameters.command = command;
        SystemParameters.configuration = configuration;
        maximumMemory = Runtime.getRuntime().maxMemory();
    }

    public static AbstractCommand getCommand()
    {
        return command;
    }

    public static Config getConfiguration() { return configuration; }

    public static int getNumberOfThreads()
    {
        return command.getNumberOfThreads();
    }

    public static int getNumberOfWorkers()
    {
        return command.getNumberOfWorkers();
    }

    public static Path getWorkingDirectory()
    {
        return command.getWorkingDirectory();
    }

    public static long getMaximumMessageSize()
    {
        return configuration.getBytes("akka.remote.maximum-payload-bytes");
    }

    public static long getMaximumMemory()
    {
        return maximumMemory;
    }

    public static long getFreeMemory()
    {
        return Runtime.getRuntime().freeMemory();
    }

    public static long getUsedMemory()
    {
        return getMaximumMemory() - getFreeMemory();
    }
}