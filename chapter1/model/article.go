package model

import (
	"log"
	"redis-with-go/chapter1/common"
	"strconv"
	"strings"
	"time"

	"github.com/gomodule/redigo/redis"
)

type Article interface {
	ArticleVote(string, string)
	PostArticle(string, string, string) string
	GetArticles(int64, string) []map[string]string
	AddRemoveGroups(string, []string, []string)
	GetGroupArticles(string, string, int64) []map[string]string
	Reset()
}

type ArticleRepo struct {
	Conn redis.Conn
}

func NewArticleRepo(conn redis.Conn) *ArticleRepo {
	return &ArticleRepo{Conn: conn}
}

func (r *ArticleRepo) ArticleVote(article, user string) {
	cutoff := time.Now().Unix() - common.OneWeekInSeconds
	if articleTime, _ := redis.Int64(r.Conn.Do("ZSCORE", "time", article)); articleTime < cutoff {
		return
	}

	articleId := strings.Split(article, ":")[1]
	if val, _ := redis.Int(r.Conn.Do("SADD", "voted:"+articleId, user)); val != 0 {
		r.Conn.Do("ZINCRBY", "score:", common.VoteScore, article)
		r.Conn.Do("HINCRBY", article, "votes", 1)
	}
}

func (r *ArticleRepo) PostArticle(user, title, link string) string {
	articleIdVal, _ := redis.Int(r.Conn.Do("INCR", "article:"))
	articleId := strconv.Itoa(articleIdVal)

	voted := "voted:" + articleId
	r.Conn.Do("SADD", voted)
	r.Conn.Do("EXPIRE", voted, common.OneWeekInSeconds*time.Second)

	now := time.Now().Unix()
	article := "article:" + articleId
	r.Conn.Do("HMSET", article, "title", title, "link", link, "poster", user, "time", now, "votes", 1)

	r.Conn.Do("ZADD", "score:", now+common.VoteScore, article)
	r.Conn.Do("ZADD", "time", now, article)
	return articleId
}

func (r *ArticleRepo) GetArticles(page int64, order string) []map[string]string {
	if order == "" {
		order = "score:"
	}
	start := (page - 1) * common.ArticlesPerPage
	end := start + common.ArticlesPerPage - 1

	ids, _ := redis.Strings(r.Conn.Do("ZREVRANGE", order, start, end))

	articles := []map[string]string{}
	for _, id := range ids {
		articleData, _ := redis.StringMap(r.Conn.Do("HGETALL", id))
		articleData["id"] = id
		articles = append(articles, articleData)
	}
	return articles
}

func (r *ArticleRepo) AddRemoveGroups(articleId string, toAdd, toRemove []string) {
	article := "article:" + articleId
	for _, group := range toAdd {
		r.Conn.Do("SADD", "group:"+group, article)
	}
	for _, group := range toRemove {
		r.Conn.Do("SREM", "group:"+group, article)
	}
}

func (r *ArticleRepo) GetGroupArticles(group, order string, page int64) []map[string]string {
	if order == "" {
		order = "score:"
	}
	key := order + group
	if val, _ := redis.Int64(r.Conn.Do("EXISTS", key)); val == 0 {
		res, _ := redis.Int64(r.Conn.Do("ZINTERSTORE", key, 2, "group:"+group, order, "AGGREGATE", "MAX"))
		if res <= 0 {
			log.Println("ZInterStore return 0")
		}
	}
	r.Conn.Do("EXPIRE", key, 60*time.Second)
	return r.GetArticles(page, key)
}

func (r *ArticleRepo) Reset() {
	r.Conn.Do("FLUSHDB")
}
