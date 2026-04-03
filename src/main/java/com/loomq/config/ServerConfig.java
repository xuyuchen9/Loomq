package com.loomq.config;

import org.aeonbits.owner.Config;

/**
 * 服务器配置
 */
@Config.Sources({"classpath:application.yml", "file:./config/application.yml"})
public interface ServerConfig extends Config {
    @DefaultValue("0.0.0.0")
    String host();

    @DefaultValue("8080")
    int port();
}
