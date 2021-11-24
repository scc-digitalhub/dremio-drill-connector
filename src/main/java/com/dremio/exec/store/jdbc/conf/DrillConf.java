/*
 * Copyright (C) 2017-2018 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.exec.store.jdbc.conf;

import static com.google.common.base.Preconditions.checkNotNull;

import com.dremio.options.OptionManager;
import com.dremio.security.CredentialsService;
import org.hibernate.validator.constraints.NotBlank;

import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.store.jdbc.CloseableDataSource;
import com.dremio.exec.store.jdbc.DataSources;
import com.dremio.exec.store.jdbc.JdbcPluginConfig;
import com.dremio.exec.store.jdbc.JdbcStoragePlugin;
import com.dremio.exec.store.jdbc.dialect.arp.ArpDialect;
import com.google.common.annotations.VisibleForTesting;

import io.protostuff.Tag;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configuration for Drill sources.
 */
@SourceType(value = "DRILL", label = "Drill", uiConfig = "drill-layout.json")
public class DrillConf extends AbstractArpConf<DrillConf> {
  private static final String ARP_FILENAME = "arp/implementation/drill-arp.yaml";
  private static final ArpDialect ARP_DIALECT =
      AbstractArpConf.loadArpFile(ARP_FILENAME, (ArpDialect::new));
  private static final String DRIVER = "org.apache.drill.jdbc.Driver";
  private static final Logger LOGGER = LoggerFactory.getLogger(DrillConf.class);

  @Tag(1)
  @DisplayMetadata(label = "Direct Connection")
  public boolean direct = false;

  @NotBlank
  @Tag(2)
  @DisplayMetadata(label = "Host")
  public String host;

  @NotBlank
  @Tag(3)
  @DisplayMetadata(label = "Port")
  public String port;

  @Tag(4)
  @DisplayMetadata(label = "Directory")
  public String directory = "drill";

  @Tag(5)
  @DisplayMetadata(label = "Cluster ID")
  public String clusterId = "drillbits1";

  @Tag(6)
  @DisplayMetadata(label = "Record fetch size")
  @NotMetadataImpacting
  public int fetchSize = 200;

  /*@Tag(7)
  @DisplayMetadata(label = "Username")
  public String username;

  @Tag(8)
  @Secret
  @DisplayMetadata(label = "Password")
  public String password;*/

  @VisibleForTesting
  public String toJdbcConnectionString() {
    final String host = checkNotNull(this.host, "Missing host.");
    final String port = checkNotNull(this.port, "Missing port.");
    final StringBuilder builder = new StringBuilder("jdbc:drill");

    if (direct) {
      builder.append(String.format(":drillbit=%s:%s", host, port));
    } else {
      builder.append(String.format(":zk=%s:%s", host, port));

      if (directory != null && directory.length() != 0) {
        builder.append(String.format("/%s", directory));
      }

      if (clusterId != null && clusterId.length() != 0) {
        builder.append(String.format("/%s", clusterId));
      }
    }

    LOGGER.info("Drill connection string is: {}", builder.toString());
    return builder.toString();
  }

  @Override
  @VisibleForTesting
  public JdbcPluginConfig buildPluginConfig(
          JdbcPluginConfig.Builder configBuilder,
          CredentialsService credentialsService,
          OptionManager optionManager
  ) {
    return configBuilder.withDialect(getDialect())
            .withDialect(getDialect())
            .withFetchSize(fetchSize)
            .withDatasourceFactory(this::newDataSource)
            .clearHiddenSchemas()
            .addHiddenSchema("information_schema", "sys")
            .withAllowExternalQuery(false)
            .build();
  }

  private CloseableDataSource newDataSource() {
    return DataSources.newGenericConnectionPoolDataSource(DRIVER,
      toJdbcConnectionString(), null, null, null, DataSources.CommitMode.DRIVER_SPECIFIED_COMMIT_MODE);
  }

  @Override
  public ArpDialect getDialect() {
    return ARP_DIALECT;
  }

  @VisibleForTesting
  public static ArpDialect getDialectSingleton() {
    return ARP_DIALECT;
  }
}
