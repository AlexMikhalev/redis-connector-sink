use anyhow::Result;
use anyhow::Context;
use async_trait::async_trait;

use fluvio::Offset;
use fluvio::consumer::Record;
use fluvio_connector_common::{Sink, LocalBoxSink, 
    tracing::{debug, error, info}
};
use url::Url;

use redis::AsyncCommands;
use redis::JsonAsyncCommands;


use crate::config::RedisConfig;

#[derive(Debug)]
pub(crate) struct RedisSink {
    pub(crate) url: Url,
}

impl RedisSink {
    pub(crate) fn new(config: &RedisConfig) -> Result<Self> {
        let url = Url::parse(&config.url.resolve()?).context("unable to parse Redis url")?;
        Ok(Self {url})
    }
}

#[async_trait]
impl Sink<Record> for RedisSink {
    async fn connect(self, _offset: Option<Offset>) -> Result<LocalBoxSink<String>> {
        info!("Connecting to Redis");
        let client = redis::Client::open(self.url.as_str())?;
        let mut con = client.get_async_connection().await?;
        info!("Connected to Redis");
        let unfold = futures::sink::unfold(con, |mut con, record: Record| async move {

            let key = record.timestamp().to_string();
            let value = String::from_utf8_lossy(record.value());
            con.json_set(key, "$".to_string(), &value).await?;
            Ok::<_, anyhow::Error>(con)
        });
        Ok(Box::pin(unfold))
    }
}