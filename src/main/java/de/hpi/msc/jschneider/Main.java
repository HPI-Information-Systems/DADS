package de.hpi.msc.jschneider;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import de.hpi.msc.jschneider.bootstrap.ActorSystemInitializer;
import de.hpi.msc.jschneider.bootstrap.command.MasterCommand;
import de.hpi.msc.jschneider.bootstrap.command.SlaveCommand;
import lombok.val;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileNotFoundException;

public class Main
{
    private static final Logger Log = LogManager.getLogger(Main.class);

    private static final String MASTER_COMMAND = "master";
    private static final String SLAVE_COMMAND = "slave";

    public static void main(final String[] args)
    {
        val masterCommand = new MasterCommand();
        val slaveCommand = new SlaveCommand();

        val argumentParser = JCommander.newBuilder()
                                       .addCommand(MASTER_COMMAND, masterCommand)
                                       .addCommand(SLAVE_COMMAND, slaveCommand)
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
                    ActorSystemInitializer.runMaster(masterCommand);
                    break;
                }
                case SLAVE_COMMAND:
                {
                    ActorSystemInitializer.runSlave(slaveCommand);
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
        catch (FileNotFoundException fileNotFoundException)
        {
            Log.error(fileNotFoundException);
            System.exit(1);
        }
        catch (Exception exception)
        {
            Log.error(exception);
            System.exit(1);
        }
    }

}
