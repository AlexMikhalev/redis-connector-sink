# WIP fluvio to RedisJSON connector

The connector dump records (assumed JSON, see fluvio smart modules) from fluvio topics into Redis JSON 

to run locally using local redis installation 

```
RUST_BACKTRACE=1 cdk test -c config-example.yaml --secrets secrets.txt
```
change REDIS_URL inside secrets.txt

For running in the infinyon cloud set REDIS_URL via `fluvio cloud secret` command
```
fluvio cloud secret set -c REDIS_URL redis://
```
to check:
```
fluvio cloud secret list
 Secret Name  Last Modified 
 REDIS_URL    2023-05-01    
```

change topic in config-example.yaml
```
meta:
  version: 0.1.0
  name: my-redis-connector-sink-test-connector
  type: redis-connector-sink-sink
  topic: hackernews
redis:
  url:
    secret:
      name: "REDIS_URL"
```
if you want to listen to the topic different to hackers news 
