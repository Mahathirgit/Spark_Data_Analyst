package com.meras.iot.core.export.pg.to.file.batch.processor.util;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class CmdUtil {
	public static String getDates(String args[]) {
		CommandLine cmd = getCommandLine(args);
		String dates = cmd.getOptionValue("dates");
		return dates;

	}

	public static String getFilePath(String args[]) {
		CommandLine cmd = getCommandLine(args);
		String objact = cmd.getOptionValue("filePath");		
		return objact;

	}

	public static CommandLine getCommandLine(String args[]) {
		CommandLineParser parser = new BasicParser();
		Options options = new Options();
		options.addOption("dates", true, "dates");
		options.addOption("filePath", true, "filePath");

		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, args);
		}
		catch (ParseException e) {
			e.printStackTrace();
		}
		return cmd;

	}

}
