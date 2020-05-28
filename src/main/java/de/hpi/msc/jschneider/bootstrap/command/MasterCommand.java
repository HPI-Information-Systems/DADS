package de.hpi.msc.jschneider.bootstrap.command;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import de.hpi.msc.jschneider.bootstrap.command.validation.ExistingFileValidator;
import de.hpi.msc.jschneider.bootstrap.command.validation.FileValidator;
import de.hpi.msc.jschneider.bootstrap.command.validation.StringToDistributionStrategyConverter;
import de.hpi.msc.jschneider.bootstrap.command.validation.StringToPathConverter;
import lombok.Getter;

import java.nio.file.Path;

@Getter
@Parameters(commandDescription = "starts a master actor system")
public class MasterCommand extends AbstractCommand
{
    private static final DistributionStrategy DEFAULT_DISTRIBUTION_STRATEGY = DistributionStrategy.HETEROGENEOUS;

    private static final float DEFAULT_WORK_LOAD_FACTOR = 0.8f;

    @Parameter(names = "--min-slaves", description = "minimum number of slaves to start processing", required = true)
    private int minimumNumberOfSlaves;

    @Parameter(names = "--sequence", description = "record sequence to analyze", required = true, converter = StringToPathConverter.class, validateValueWith = ExistingFileValidator.class)
    private Path sequenceFilePath;

    @Parameter(names = "--sub-sequence-length", description = "length for the sub sequences", required = true)
    private int subSequenceLength;

    @Parameter(names = "--convolution-size", description = "size of the local convolution to transform subsequences into vectors", required = true)
    private int convolutionSize;

    @Parameter(names = "--intersection-segments", description = "number of intersection segments to create for the node creation", required = true)
    private int numberOfIntersectionSegments;

    @Parameter(names = "--query-length", description = "length of paths to score", required = true)
    private int queryPathLength;

    @Parameter(names = "--output", description = "file to write resulting normality scores to", required = true, converter = StringToPathConverter.class, validateValueWith = FileValidator.class)
    private Path outputFilePath;

    @Parameter(names = "--distribution", description = "strategy for distributing the workload", required = false, converter = StringToDistributionStrategyConverter.class)
    private DistributionStrategy distributionStrategy = DEFAULT_DISTRIBUTION_STRATEGY;

    @Parameter(names = "--work-load-factor", description = "portion of main memory that is contributed to the usual calculation process", required = false)
    private float workLoadFactor = DEFAULT_WORK_LOAD_FACTOR;
}
