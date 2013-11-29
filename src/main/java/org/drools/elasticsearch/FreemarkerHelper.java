package org.drools.elasticsearch;

import java.io.OutputStreamWriter;
import java.util.Map;

import org.apache.commons.io.output.ByteArrayOutputStream;

import freemarker.template.Configuration;
import freemarker.template.Template;

public class FreemarkerHelper {
	@SuppressWarnings("rawtypes")
	public static String parse(String templateFile, Map replacements) {
		try {
			Configuration cfg = new Configuration();
			Template template = cfg.getTemplate("src/main/resources/" + templateFile);
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			template.process(replacements, new OutputStreamWriter(out));
			out.flush();
			return out.toString();
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}
}
