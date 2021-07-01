require 'date'

def register(params)
end

def filter(event)
    @kafka_latency = DateTime.now.strftime('%Q') - event.get("ingestion_ms").size
    event.set('kafka_latency', @kafka_latency)
	return [event]
end