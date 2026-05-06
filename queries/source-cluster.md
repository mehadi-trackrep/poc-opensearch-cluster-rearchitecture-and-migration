GET _search
{
  "query": {
    "match_all": {}
  }
}

GET /_cluster/health

GET mmh-poc/_search
{
  "query": {
    "range": {
      "indexed_at": {
        "gte": "2026-05-04"
      }
    }
  },
  "sort": [
    {
      "orgno": {
        "order": "desc"
      }
    }
  ]
}

GET mmh-poc/_mapping
GET _cluster/settings
GET /_cat/indices?v&h=health,status,index,pri,rep,docs.count,store.size

GET /_cat/shards?v

GET /_cat/nodes?v&h=name,host,heap.percent,heap.current,heap.max,ram.percent,cpu

GET /_nodes/stats/jvm?pretty
