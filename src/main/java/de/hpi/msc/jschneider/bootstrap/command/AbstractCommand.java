package de.hpi.msc.jschneider.bootstrap.command;

import com.beust.jcommander.Parameter;
import de.hpi.msc.jschneider.bootstrap.command.validation.DirectoryValidator;
import de.hpi.msc.jschneider.bootstrap.command.validation.FileValidator;
import de.hpi.msc.jschneider.bootstrap.command.validation.StringToPathConverter;
import lombok.Getter;

import java.nio.file.Path;

@Getter
public abstract class AbstractCommand
{
    private static final int DEFAULT_NUMBER_OF_THREADS = 4;
    private static final int DEFAULT_NUMBER_OF_WORKERS = 4;
    protected static final int DEFAULT_PORT = 7788;
    private static final boolean DEFAULT_DISABLE_MESSAGE_EXCHANGE = false;
    private static final boolean DEFAULT_DISABLE_STATISTICS = false;
    private static final boolean DEFAULT_DISABLE_SEQUENCE_MATRIX = false;

    @Parameter(names = "--host", description = "host address of THIS actor system", required = true)
    private String host;

    @Parameter(names = "--port", description = "port of THIS actor system")
    private int port = DEFAULT_PORT;

    @Parameter(names = "--threads", description = "number of threads to utilize")
    private int numberOfThreads = DEFAULT_NUMBER_OF_THREADS;

    @Parameter(names = "--workers", description = "number of workers to spawn")
    private int numberOfWorkers = DEFAULT_NUMBER_OF_WORKERS;

    @Parameter(names = "--working-dir", description = "working directory", required = true, converter = StringToPathConverter.class, validateValueWith = DirectoryValidator.class)
    private Path workingDirectory;

    @Parameter(names = "--statistics-output", description = "file to store statistics in", required = false, converter = StringToPathConverter.class, validateValueWith = FileValidator.class)
    private Path statisticsFile;

    @Parameter(names = "--no-message-exchange", description = "disables the MessageExchange protocol", required = false)
    private boolean disableMessageExchange = DEFAULT_DISABLE_MESSAGE_EXCHANGE;

    @Parameter(names = "--no-statistics", description = "disables the Statistics protocol", required = false)
    private boolean disableStatistics = DEFAULT_DISABLE_STATISTICS;

    @Parameter(names = "--no-sequence-matrix", description = "disables the SequenceMatrix optimization", required = false)
    private boolean disableSequenceMatrix = DEFAULT_DISABLE_SEQUENCE_MATRIX;
}
