package doppio

import (
	"bytes"
	"fmt"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/infamousity/distributed-cache/internal/cache"
	"github.com/infamousity/distributed-cache/internal/cluster"
	"github.com/infamousity/distributed-cache/internal/log"
)

type Server struct {
	bindAddr string
	ring     cluster.Node
	local    *cache.Cache
	logger   log.Interface
	client   *retryablehttp.Client
}

func New(bindAddr string, ring cluster.Node) *Server {
	client := retryablehttp.NewClient()
	client.Logger = log.Default()
	return &Server{
		bindAddr: bindAddr,
		ring:     ring,
		local:    cache.New(), // safely initializes the singleton Ristretto instance
		logger:   log.Default(),
		client:   client,
	}
}

func (s *Server) Run() error {
	gin.SetMode(gin.ReleaseMode)
	r := gin.Default()
	_ = r.SetTrustedProxies(nil)

	r.PUT("/cache/:key", s.handlePut)
	r.GET("/cache/:key", s.handleGet)
	r.DELETE("/cache/:key", s.handleDelete)

	s.logger.Infof("Starting Doppio server on %s", s.bindAddr)
	return r.Run(s.bindAddr)
}

func (s *Server) handlePut(c *gin.Context) {
	l := log.Default()
	key := c.Param("key")
	value, err := c.GetRawData()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return
	}

	// Forward if not responsible
	if node, ok := s.ring.Get(key); ok && node != s.ring.GetSelf() {
		targetNode := node
		if node, ok = s.ring.GetForwardAddr(targetNode); ok {
			l.Debugf("Forwarding PUT for key %s from %s => %s (%s)", key, s.ring.GetSelf(), targetNode, node)
			s.forwardRequest(http.MethodPut, node, key, value, c)
		}
		return
	}

	if ok := s.local.Set(key, value, 1); !ok {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to store value"})
		return
	}

	c.Status(http.StatusCreated)
}

func (s *Server) handleGet(c *gin.Context) {
	l := log.Default()
	key := c.Param("key")

	if node, ok := s.ring.Get(key); ok && node != s.ring.GetSelf() {
		targetNode := node
		if node, ok = s.ring.GetForwardAddr(targetNode); ok {
			l.Debugf("Forwarding GET for key %s from %s => %s (%s)", key, s.ring.GetSelf(), targetNode, node)
			s.forwardRequest(http.MethodGet, node, key, nil, c)
		}
		return
	}

	if value, ok := s.local.Get(key); ok {
		c.Data(http.StatusOK, "application/octet-stream", value.([]byte))
		return
	}

	c.JSON(http.StatusNotFound, gin.H{"error": "key not found"})
}

func (s *Server) handleDelete(c *gin.Context) {
	l := log.Default()
	key := c.Param("key")

	if node, ok := s.ring.Get(key); ok && node != s.ring.GetSelf() {
		targetNode := node
		if node, ok = s.ring.GetForwardAddr(targetNode); ok {
			l.Debugf("Forwarding DELETE for key %s from %s => %s (%s)", key, s.ring.GetSelf(), targetNode, node)
			s.forwardRequest(http.MethodDelete, node, key, nil, c)
		}
		return
	}

	s.local.Del(key)
	c.Status(http.StatusNoContent)
}

func (s *Server) forwardRequest(method, node, key string, body []byte, c *gin.Context) {
	url := fmt.Sprintf("http://%s/cache/%s", node, key)
	s.logger.Infof("Forwarding %s to %s", method, url)

	var reqBody io.Reader
	if len(body) > 0 {
		reqBody = bytes.NewReader(body)
	}

	req, err := retryablehttp.NewRequestWithContext(c.Request.Context(), method, url, reqBody)
	if err != nil {
		s.logger.Errorf("Failed to create forward request: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "forwarding failed"})
		return
	}

	resp, err := s.client.Do(req)
	if err != nil {
		s.logger.Errorf("Failed to forward to %s: %v", node, err)
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "forwarding failed"})
		return
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	// Copy status and body back to client
	c.Status(resp.StatusCode)
	bodyBytes, _ := io.ReadAll(resp.Body)
	_, _ = c.Writer.Write(bodyBytes)
}
