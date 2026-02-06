package api

import (
	"bytes"
	"encoding/base64"
	"encoding/gob"
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
	cluster  *cluster.Cluster
	logger   log.Interface
	client   *retryablehttp.Client
	isTLS    bool
}

func New(bindAddr string, cluster *cluster.Cluster) *Server {
	client := retryablehttp.NewClient()
	client.Logger = log.Default()

	return &Server{
		bindAddr: bindAddr,
		cluster:  cluster,
		logger:   log.Default(),
		client:   client,
	}
}

func (s *Server) Run() error {
	gin.SetMode(gin.ReleaseMode)
	r := gin.Default()
	_ = r.SetTrustedProxies(nil)

	r.POST("/cache", s.handlePost)
	r.Handle("LIST", "/cache", s.handleList)
	r.PUT("/cache/:key", s.handlePut)
	r.GET("/cache/:key", s.handleGet)
	r.DELETE("/cache/:key", s.handleDelete)
	r.GET("/members", func(c *gin.Context) {
		out := s.cluster.GetNode().List()
		c.JSON(http.StatusOK, out)
	})
	// metrics
	r.GET("/load", func(c *gin.Context) {
		out := s.cluster.GetNode().LoadDistribution()
		c.JSON(http.StatusOK, out)
	})
	r.GET("/export", s.handleExport)
	s.isTLS = s.cluster.IsTLS()
	if s.isTLS {
		s.logger.Infof("Starting https api server on %s", s.bindAddr)
		return r.RunTLS(s.bindAddr, s.cluster.CertFile(), s.cluster.KeyFile())
	}
	s.logger.Infof("Starting http api server on %s", s.bindAddr)
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
	if node, ok := s.cluster.GetNode().Get(key); ok && node != s.cluster.GetNode().GetSelf() {
		targetNode := node
		if node, ok = s.cluster.GetNode().GetForwardAddr(targetNode); ok {
			l.Debugf("Forwarding PUT for key %s from %s => %s (%s)", key, s.cluster.GetNode().GetSelf(), targetNode, node)
			s.forwardRequest(http.MethodPut, node, key, value, c)
		}
		return
	}

	if ok := s.cluster.LocalCache().Set(key, value, 1); !ok {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to store value"})
		return
	}

	c.Status(http.StatusCreated)
}

func (s *Server) handlePost(c *gin.Context) {
	load := make([]*cache.ItemDTO[any], 0)
	err := c.ShouldBindJSON(&load)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return
	}
	if len(load) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing keys"})
		return
	}
	s.logger.With("load", load).Info("Got POST request")
	items := make([]*cache.Item[any], len(load))
	for i, item := range load {
		items[i] = item.ToItem()
	}
	buf := new(bytes.Buffer)
	if err = gob.NewEncoder(buf).Encode(items); err != nil {
		s.logger.Errorf("Failed to encode items: %v", err)
		c.AbortWithStatus(http.StatusBadRequest)
		return
	} else {
		if err = s.cluster.LocalCache().UnmarshalBinary(buf.Bytes()); err != nil {
			s.logger.Errorf("Failed to unmarshal items: %v", err)
			c.AbortWithStatus(http.StatusBadRequest)
			return
		}
		s.logger.Infof("Loaded %d items", len(items))
	}
	c.Status(http.StatusAccepted)
}

func (s *Server) handleGet(c *gin.Context) {
	key := c.Param("key")

	if node, ok := s.cluster.GetNode().Get(key); ok && node != s.cluster.GetNode().GetSelf() {
		targetNode := node
		if node, ok = s.cluster.GetNode().GetForwardAddr(targetNode); ok {
			s.logger.Debugf("Forwarding GET for key %s from %s => %s (%s)", key, s.cluster.GetNode().GetSelf(), targetNode, node)
			s.forwardRequest(http.MethodGet, node, key, nil, c)
		}
		return
	}
	s.logger.Debugf("Processing GET for key %s", key)

	if value, ok := s.cluster.LocalCache().Get(key); ok {
		c.Data(http.StatusOK, "application/octet-stream", value.([]byte))
		return
	}

	c.JSON(http.StatusNotFound, gin.H{"error": "key not found"})
}

func (s *Server) handleList(c *gin.Context) {
	key := ""
	bkey, err := c.GetRawData()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return
	}
	if bytes.HasPrefix(bkey, []byte("b64:")) {
		if decodedKey, derr := base64.RawURLEncoding.DecodeString(string(bkey[4:])); derr != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body: raw base64url encoding expected"})
			return
		} else {
			key = string(decodedKey)
		}
	} else {
		key = string(bkey)
	}
	s.logger.With("key", key).Info("Got LIST request")
	if node, ok := s.cluster.GetNode().Get(key); ok && node != s.cluster.GetNode().GetSelf() {
		targetNode := node
		if node, ok = s.cluster.GetNode().GetForwardAddr(targetNode); ok {
			s.logger.Debugf("Forwarding LIST for key %s from %s => %s (%s)", key, s.cluster.GetNode().GetSelf(), targetNode, node)
			s.forwardRequest("LIST", node, key, bkey, c)
		}
		return
	}

	if value, ok := s.cluster.LocalCache().Get(key); ok {
		c.Data(http.StatusOK, "application/octet-stream", value.([]byte))
		return
	}

	c.JSON(http.StatusNotFound, gin.H{"error": "key not found"})
}

func (s *Server) handleDelete(c *gin.Context) {
	l := log.Default()
	key := c.Param("key")

	if node, ok := s.cluster.GetNode().Get(key); ok && node != s.cluster.GetNode().GetSelf() {
		targetNode := node
		if node, ok = s.cluster.GetNode().GetForwardAddr(targetNode); ok {
			l.Debugf("Forwarding DELETE for key %s from %s => %s (%s)", key, s.cluster.GetNode().GetSelf(), targetNode, node)
			s.forwardRequest(http.MethodDelete, node, key, nil, c)
		}
		return
	}

	s.cluster.LocalCache().Del(key)
	c.Status(http.StatusNoContent)
}

func (s *Server) handleExport(c *gin.Context) {
	s.logger.Infof("Exporting cache")
	data, err := s.cluster.LocalCache().MarshalBinary()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to export cache"})
		return
	}
	if len(data) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing keys"})
		return
	}
	c.Data(http.StatusOK, "application/octet-stream", data)
	go func() {
		s.logger.Infof("Trimming cache: %d", s.cluster.LocalCache().MaxCost()-s.cluster.LocalCache().RemainingCost())
		err = s.cluster.Trim(data)
		if err != nil {
			s.logger.Errorf("Failed to trim cache: %v", err)
		} else {
			s.logger.Infof("Trimmed cache: %d", s.cluster.LocalCache().MaxCost()-s.cluster.LocalCache().RemainingCost())
		}
	}()
}

func (s *Server) forwardRequest(method, node, key string, body []byte, c *gin.Context) {
	var path string
	if method == "LIST" {
		path = "cache"
	} else {
		path = fmt.Sprintf("cache/%s", key)

	}
	scheme := "http"
	if s.isTLS {
		scheme = "https"
	}
	url := fmt.Sprintf("%s://%s/%s", scheme, node, path)

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
