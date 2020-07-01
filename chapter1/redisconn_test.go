package main

import (
	"redis-with-go/chapter1/model"
	"redis-with-go/redisconn"
	"testing"

	"github.com/gomodule/redigo/redis"
)

func Test(t *testing.T) {
	conn := redisconn.ConnectRedis()
	client := model.NewArticleRepo(conn)

	articleId := client.PostArticle("username", "A title", "http://www.google.com")
	t.Log("We posted a new article with id: ", articleId)
	assertStringResult(t, "1", articleId)

	r, _ := redis.Strings(client.Conn.Do("HGETALL", "article:"+articleId))
	t.Log("\nIts HASH looks like: ", r)
	assertTrue(t, len(r) != 0)

	client.ArticleVote("article:"+articleId, "other_user")
	v, _ := redis.Int(client.Conn.Do("HGET", "article:"+articleId, "votes"))
	t.Log("\nWe voted for the article, it now has votes: ", v)
	assertTrue(t, v >= 1)

	t.Log("\nThe currently highest-scoring articles are: ")
	articles := client.GetArticles(1, "")
	assertTrue(t, len(articles) >= 1)
	for k, v := range articles {
		t.Log(k, v)
	}

	client.AddRemoveGroups(articleId, []string{"new-group"}, []string{})
	articles = client.GetGroupArticles("new-group", "score:", 1)
	t.Log("\nWe added the article to a new group, other articles include: ")
	assertTrue(t, len(articles) >= 1)
	for k, v := range articles {
		t.Log(k, v)
	}
	defer client.Reset()
}

func assertStringResult(t *testing.T, want, get string) {
	t.Helper()
	if want != get {
		t.Errorf("want get %v, actual get %v\n", want, get)
	}
}

func assertTrue(t *testing.T, v bool) {
	t.Helper()
	if v != true {
		t.Error("assert true but get a false value")
	}
}
