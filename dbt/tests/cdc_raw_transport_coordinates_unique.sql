-- event_id is the sink key, but the raw contract also declares the concrete
-- Kafka transport coordinate unique.
select
    kafka_topic,
    kafka_partition,
    kafka_offset,
    count(*) as event_count
from {{ source('bronze', 'postgres_cdc_events') }}
group by kafka_topic, kafka_partition, kafka_offset
having count(*) <> 1
