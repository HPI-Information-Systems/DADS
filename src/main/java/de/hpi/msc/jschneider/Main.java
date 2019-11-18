package de.hpi.msc.jschneider;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import de.hpi.msc.jschneider.bootstrap.command.MasterCommand;
import lombok.val;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Main
{
    private static final Logger Log = LogManager.getLogger(Main.class);

    private static final String MASTER_COMMAND = "master";
    private static final String SLAVE_COMMAND = "slave";

    public static void main(final String[] args)
    {
        val masterCommand = new MasterCommand();

        val argumentParser = JCommander.newBuilder()
                                       .addCommand(MASTER_COMMAND, masterCommand)
                                       .build();

        try
        {
            argumentParser.parse(args);
            val parsedCommand = argumentParser.getParsedCommand();

            if (parsedCommand == null)
            {
                throw new ParameterException("Unable to parse command!");
            }

            switch (parsedCommand)
            {
                case MASTER_COMMAND:
                {
                    break;
                }
                case SLAVE_COMMAND:
                {
                    break;
                }
                default:
                {
                    throw new ParameterException("Unknown command!");
                }
            }
        }
        catch (ParameterException parameterException)
        {
            Log.error(parameterException);
            if (argumentParser.getParsedCommand() == null)
            {
                argumentParser.usage();
            }
            else
            {
                argumentParser.usage(argumentParser.getParsedCommand());
            }
            System.exit(1);
        }
    }

}
