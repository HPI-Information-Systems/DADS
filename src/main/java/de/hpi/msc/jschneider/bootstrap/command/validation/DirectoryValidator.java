package de.hpi.msc.jschneider.bootstrap.command.validation;

import com.beust.jcommander.IValueValidator;
import com.beust.jcommander.ParameterException;
import lombok.val;

import java.nio.file.Path;

public class DirectoryValidator implements IValueValidator<Path>
{
    @Override
    public void validate(final String parameterName, final Path parameterValue) throws ParameterException
    {
        val file = parameterValue.toFile();
        if (!file.exists() && !file.mkdirs())
        {
            throw new ParameterException(String.format("Unable to create \"%1$s\"!", parameterValue));
        }

        if (!file.isDirectory())
        {
            throw new ParameterException(String.format("%1$s must specify a directory!", parameterName));
        }
    }
}
