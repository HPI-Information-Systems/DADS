package de.hpi.msc.jschneider.bootstrap.command.validation;

import com.beust.jcommander.IValueValidator;
import com.beust.jcommander.ParameterException;
import lombok.val;

import java.nio.file.Path;

public class FileValidator implements IValueValidator<Path>
{
    @Override
    public void validate(final String parameterName, final Path parameterValue) throws ParameterException
    {
        val file = parameterValue.toFile();
        if (!file.exists())
        {
            throw new ParameterException(String.format("\"%1$s\" can not be found!", parameterValue));
        }

        if (!file.isFile())
        {
            throw new ParameterException(String.format("%1$s must specify a file!", parameterName));
        }
    }
}
