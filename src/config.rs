use fluvio_connector_common::{connector, secret::SecretString};

#[connector(config,name = "redis")]
#[derive(Debug)]
pub(crate) struct RedisConfig {
    #[allow(dead_code)]
    pub url: SecretString,
}