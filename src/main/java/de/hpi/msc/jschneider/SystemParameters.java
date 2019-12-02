package de.hpi.msc.jschneider;

import de.hpi.msc.jschneider.bootstrap.command.AbstractCommand;

import java.nio.file.Path;

public class SystemParameters
{
    private static AbstractCommand command;
    private static long maximumMemory;

    public static void initialize(AbstractCommand command)
    {
        SystemParameters.command = command;
        maximumMemory = Runtime.getRuntime().maxMemory();
    }

    public static AbstractCommand getCommand()
    {
        return command;
    }

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
