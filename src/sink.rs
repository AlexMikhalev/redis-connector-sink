use anyhow::Context;
use anyhow::Result;
use async_trait::async_trait;

use fluvio::consumer::Record;
use fluvio::Offset;
use fluvio_connector_common::{
    tracing::{debug, error, info},
    LocalBoxSink, Sink,
};
use url::Url;

use redis::AsyncCommands;
use redis::JsonAsyncCommands;

use crate::config::RedisConfig;

use once_cell::sync::OnceCell;

#[derive(Debug, Clone)]
pub(crate) struct RedisSink {
    pub(crate) prefix: String,
    pub(crate) url: Url,
    pub(crate) to_hash: Option<bool>,
}

static PREFIX: OnceCell<String> = OnceCell::new();
static REDIS_SINK_CONFIG: OnceCell<RedisSink> = OnceCell::new();
impl RedisSink {
    pub(crate) fn new(config: &RedisConfig) -> Result<Self> {
        let prefix = config.prefix.clone();
        let url = Url::parse(&config.url.resolve()?).context("unable to parse Redis url")?;
        let to_hash = config.to_hash;
        REDIS_SINK_CONFIG.set(RedisSink {
            prefix: config.prefix.clone(),
            url: url.clone(),
            to_hash: to_hash,
        });

        Ok(Self {
            prefix,
            url,
            to_hash,
        })
    }
}

#[async_trait]
impl Sink<Record> for RedisSink {
    async fn connect(self, _offset: Option<Offset>) -> Result<LocalBoxSink<Record>> {
        info!("Connecting to Redis");
        let client = redis::Client::open(self.url.as_str())?;
        let con = client.get_async_connection().await?;
        info!("Connected to Redis");
        info!("Prefix: {}", PREFIX.get().unwrap());
        let unfold = futures::sink::unfold(con, |mut con, record: Record| async move {
            let key = if let Some(key) = record.key() {
                String::from_utf8_lossy(key).to_string()
            } else {
                record.timestamp().to_string()
            };
            let key = format!("{}:{}", REDIS_SINK_CONFIG.get().unwrap().prefix, key);
            println!("key: {}", key);
            let value = String::from_utf8_lossy(record.value());
            if let Some(true) = REDIS_SINK_CONFIG.get().unwrap().to_hash {
                info!("Using set");
                redis::cmd("SET")
                    .arg(&[key, value.to_string()])
                    .query_async(&mut con)
                    .await?;
            } else {
                con.json_set(key, "$".to_string(), &value).await?;
            }

            Ok::<_, anyhow::Error>(con)
        });
        Ok(Box::pin(unfold))
    }
}
