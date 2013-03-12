package com.db.exporter.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class AppContext {
	private static ApplicationContext context;

	static Log logger = LogFactory.getLog(AppContext.class);

	public static void destroyContext() {

		((ClassPathXmlApplicationContext) context).close();
		context = null;
	}

	public ApplicationContext getContext() {

		if (context == null) {
			context = new ClassPathXmlApplicationContext(
					new String[] { "applicationContext.xml" });

		}
		return context;
	}
}
