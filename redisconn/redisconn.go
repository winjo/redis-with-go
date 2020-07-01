package redisconn

import (
	"log"

	"github.com/gomodule/redigo/redis"
)

func ConnectRedis() redis.Conn {
	conn, err := redis.Dial("tcp", ":6379", redis.DialPassword("123"), redis.DialDatabase(15))

	if err != nil {
		log.Fatalf("Connect to redis client failed, err: %v\n", err)
	}

	if _, err := conn.Do("PING"); err != nil {
		log.Fatalf("Connect to redis client failed, err: %v\n", err)
	}
	return conn
}
