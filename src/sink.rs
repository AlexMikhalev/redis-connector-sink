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

#[derive(Debug,Clone)]
pub(crate) struct RedisSink {
    pub(crate) prefix: String,
    pub(crate) url: Url,
    pub(crate) to_hash: Option<bool>,
}

impl RedisSink {
    pub(crate) fn new(config: &RedisConfig) -> Result<Self> {
        let prefix = config.prefix.clone();
        let url = Url::parse(&config.url.resolve()?).context("unable to parse Redis url")?;
        let to_hash = config.to_hash;
        Ok(Self {prefix, url, to_hash})
    }
}

#[async_trait]
impl Sink<Record> for RedisSink {
    async fn connect(self, _offset: Option<Offset>) -> Result<LocalBoxSink<Record>> {
        info!("Connecting to Redis");
        let client = redis::Client::open(self.url.as_str())?;
        let con = client.get_async_connection().await?;
        info!("Connected to Redis");
        let prefix= self.prefix.clone().to_owned();
        info!("Prefix: {}", prefix);
        let unfold = futures::sink::unfold(con, |mut con, record: Record| async move {
            let key = if let Some(key) = record.key() {
                String::from_utf8_lossy(key).to_string()
            }else{
                record.timestamp().to_string()
            };
            let key = format!("{}:{}", prefix, key);
            let value = String::from_utf8_lossy(record.value());
            con.json_set(key, "$".to_string(), &value).await?;
            Ok::<_, anyhow::Error>(con)
        });
        Ok(Box::pin(unfold))
    }
}