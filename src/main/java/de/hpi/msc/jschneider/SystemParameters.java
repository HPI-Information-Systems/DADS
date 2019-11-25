package de.hpi.msc.jschneider;

import de.hpi.msc.jschneider.bootstrap.command.AbstractCommand;

import java.nio.file.Path;

public class SystemParameters
{
    private static int numberOfThreads;
    private static int numberOfWorkers;
    private static Path workingDirectory;
    private static long maximumMemory;

    public static void initialize(AbstractCommand command)
    {
        numberOfThreads = command.getNumberOfThreads();
        numberOfWorkers = command.getNumberOfWorkers();
        workingDirectory = command.getWorkingDirectory();
        maximumMemory = Runtime.getRuntime().maxMemory();
    }

    public static int getNumberOfThreads()
    {
        return numberOfThreads;
    }

    public static int getNumberOfWorkers()
    {
        return numberOfWorkers;
    }

    public static Path getWorkingDirectory()
    {
        return workingDirectory;
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
