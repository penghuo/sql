package com.amazon.opendistroforelasticsearch.sql.expression.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({ExpressionConfig.class})
public class CoreEngineConfig {


}
