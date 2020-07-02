package model

import (
	"crypto"
	"encoding/hex"
	"encoding/json"
	"log"
	"net/url"
	"redis-with-go/chapter2/common"
	"redis-with-go/chapter2/repository"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gomodule/redigo/redis"
)

type Client struct {
	Conn redis.Conn
}

func NewClient(conn redis.Conn) *Client {
	return &Client{Conn: conn}
}

func (r *Client) CheckToken(token string) string {
	val, _ := redis.String(r.Conn.Do("HGET", "login:", token))
	return val
}

func (r *Client) UpdateToken(token, user, item string) {
	timestamp := time.Now().Unix()
	r.Conn.Do("HSET", "login:", token, user)
	r.Conn.Do("ZADD", "recent", timestamp, token)
	if item != "" {
		r.Conn.Do("ZADD", "viewed:"+token, item, timestamp)
		r.Conn.Do("ZREMRANGEBYRANK", "viewed:"+token, 0, -26)
	}
}

func (r *Client) CleanSessions() {
	for !common.QUIT {
		size, _ := redis.Int64(r.Conn.Do("ZCARD", "recent:"))
		if size <= common.LIMIT {
			time.Sleep(1 * time.Second)
			continue
		}

		endIndex := Min(size-common.LIMIT, 100)
		tokens, _ := redis.Strings(r.Conn.Do("ZRANGE", "recent:", 0, endIndex-1))

		var sessionKey []string
		for _, token := range tokens {
			sessionKey = append(sessionKey, token)
		}

		r.Conn.Do("DEL", redis.Args{}.AddFlat(sessionKey))
		r.Conn.Do("HDEL", "login:", redis.Args{}.AddFlat(tokens))
		r.Conn.Do("ZREM", "recent:", tokens)
	}
	defer atomic.AddInt32(&common.FLAG, -1)
}

func (r *Client) AddToCart(session, item string, count int) {
	switch {
	case count <= 0:
		r.Conn.Do("HDEL", "cart:"+session, item)
	default:
		r.Conn.Do("HSET", "cart:"+session, item, count)
	}
}

func (r *Client) CleanFullSession() {
	for !common.QUIT {
		size, _ := redis.Int64(r.Conn.Do("ZCARD", "recent:"))
		if size <= common.LIMIT {
			time.Sleep(1 * time.Second)
			continue
		}

		endIndex := Min(size-common.LIMIT, 100)
		tokens, _ := redis.Strings(r.Conn.Do("ZRANGE", "recent:", 0, endIndex-1))

		var sessionKey []string
		for _, token := range tokens {
			sessionKey = append(sessionKey, "viewed:"+token)
			sessionKey = append(sessionKey, "cart:"+token)
		}

		r.Conn.Do("DEL", redis.Args{}.AddFlat(sessionKey))
		r.Conn.Do("HDEL", "login:", redis.Args{}.AddFlat(tokens))
		r.Conn.Do("ZREM", "recent:", tokens)
	}
	defer atomic.AddInt32(&common.FLAG, -1)
}

func (r *Client) CacheRequest(request string, callback func(string) string) string {
	if !r.CanCache(request) {
		return callback(request)
	}

	pageKey := "cache:" + hashRequest(request)
	content, _ := redis.String(r.Conn.Do("GET", pageKey))

	if content == "" {
		content = callback(request)
		r.Conn.Do("SET", pageKey, content, "ex", 300)
	}
	return content
}

func (r *Client) ScheduleRowCache(rowId string, delay int64) {
	r.Conn.Do("ZADD", "delay:", delay, rowId)
	r.Conn.Do("ZADD", "schedule:", time.Now().Unix(), rowId)
}

func (r *Client) CacheRows() {
	for !common.QUIT {
		var next []struct {
			Member string
			Score  int64
		}
		val, _ := redis.Values(r.Conn.Do("ZRANGE", "schedule:", 0, 0, "WITHSCORES"))
		_ = redis.ScanSlice(val, &next)
		now := time.Now().Unix()
		if len(next) == 0 || next[0].Score > now {
			time.Sleep(50 * time.Millisecond)
			continue
		}

		rowId := next[0].Member
		delay, _ := redis.Int64(r.Conn.Do("ZSCORE", "delay:", rowId))
		if delay <= 0 {
			r.Conn.Do("ZREM", "delay:", rowId)
			r.Conn.Do("ZREM", "schedule:", rowId)
			r.Conn.Do("DEL", "inv:"+rowId)
			continue
		}

		row := repository.Get(rowId)
		r.Conn.Do("ZADD", "schedule:", now+delay, rowId)
		jsonRow, err := json.Marshal(row)
		if err != nil {
			log.Fatalf("marshal json failed, data is: %v, err is: %v\n", row, err)
		}
		r.Conn.Do("SET", "inv:"+rowId, jsonRow)
	}
	defer atomic.AddInt32(&common.FLAG, -1)
}

func Min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func (r *Client) UpdateTokenModified(token, user string, item string) {
	timestamp := time.Now().Unix()
	r.Conn.Do("HSET", "login:", token, user)
	r.Conn.Do("ZADD", "recent:", token, timestamp)
	if item != "" {
		r.Conn.Do("ZADD", "viewed:"+token, item, timestamp)
		r.Conn.Do("ZREMRANGEBYRANK", "viewed:"+token, 0, -26)
		r.Conn.Do("ZINCRBY", "viewed:", -1, item)
	}
}

func (r *Client) RescaleViewed() {
	for !common.QUIT {
		r.Conn.Do("ZREMRANGEBYRANK", "viewed:", 20000, -1)
		r.Conn.Do("ZINTERSTORE", "viewed:", 1, "viewd:", "WEIGHTS", 0.5)
		time.Sleep(300 * time.Second)
	}
}

func (r *Client) CanCache(request string) bool {
	itemId := extractItemId(request)
	if itemId == "" || isDynamic(request) {
		return false
	}
	val, _ := r.Conn.Do("ZRANK", "viewed:", itemId)
	if val == nil {
		return false
	}
	rank, _ := redis.Int64(val, nil)
	return rank < 10000
}

func (r *Client) Reset() {
	r.Conn.Do("FLUSHDB")

	common.QUIT = false
	common.LIMIT = 10000000
	common.FLAG = 1
}

func extractItemId(request string) string {
	parsed, _ := url.Parse(request)
	queryValue, _ := url.ParseQuery(parsed.RawQuery)
	query := queryValue.Get("item")
	return query
}

func isDynamic(request string) bool {
	parsed, _ := url.Parse(request)
	queryValue, _ := url.ParseQuery(parsed.RawQuery)
	for k := range queryValue {
		if strings.Contains(k, "_") {
			return true
		}
	}
	return false
}

func hashRequest(request string) string {
	hash := crypto.MD5.New()
	hash.Write([]byte(request))
	res := hash.Sum(nil)
	return hex.EncodeToString(res)
}
