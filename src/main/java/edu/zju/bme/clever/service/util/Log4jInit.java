package edu.zju.bme.clever.service.util;

import java.io.FileInputStream;
import java.util.Properties;

import javax.servlet.ServletConfig;
import javax.servlet.http.HttpServlet;

import org.apache.log4j.PropertyConfigurator;

public class Log4jInit extends HttpServlet {
	
	private static final long serialVersionUID = 9121426914301154984L;

	public Log4jInit() {
	}

    @Override
	public void init(ServletConfig config) {
		String prefix = config.getServletContext().getRealPath("/");
		String file = config.getInitParameter("log4j");
		String filePath = prefix + file;
		System.out.println("log4j.properties path: " + filePath);
		Properties props = new Properties();
		try {
			FileInputStream iStream = new FileInputStream(filePath);
			props.load(iStream);
			String logFile = prefix + props.getProperty("log4j.appender.R.File");
			System.out.println("log file path: " + logFile);
			props.setProperty("log4j.appender.R.File", logFile);
			PropertyConfigurator.configure(props);
		} catch (Exception e) {
		}
	}

}
