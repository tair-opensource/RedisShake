package client

import (
	"RedisShake/internal/client/proto"
	"RedisShake/internal/log"
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"strings"
)

func EncodeArgv(argv []string, buf *bytes.Buffer) {
	writer := proto.NewWriter(buf)
	argvInterface := make([]interface{}, len(argv))

	for inx, item := range argv {
		argvInterface[inx] = item
	}
	err := writer.WriteArgs(argvInterface)
	if err != nil {
		log.Panicf(err.Error())
	}
}

// IsCluster is for determining whether the server is in cluster mode.
func (r *Redis) IsCluster() bool {
	reply := r.DoWithStringReply("INFO", "Cluster")
	return strings.Contains(reply, "cluster_enabled:1")
}

// createTLSConfig 根据配置创建 TLS 配置
func CreateTLSConfig(keyFilePath, CACertFilePath, certFilePath string) (*tls.Config, error) {
	if keyFilePath == "" && CACertFilePath == "" && certFilePath == "" {
		return &tls.Config{
			InsecureSkipVerify: true, // 如果没有配置，默认不验证
		}, nil
	}

	config := &tls.Config{
		MinVersion: tls.VersionTLS12, // 设置最低 TLS 版本
	}

	// 加载 CA 证书(如果配置)
	if CACertFilePath != "" {
		caCert, err := ioutil.ReadFile(CACertFilePath)
		if err != nil {
			return nil, err
		}

		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to append CA cert")
		}
		config.RootCAs = caCertPool
	}

	// 加载客户端证书和私钥(如果配置，用于双向认证)
	if certFilePath != "" && keyFilePath != "" {
		cert, err := tls.LoadX509KeyPair(certFilePath, keyFilePath)
		if err != nil {
			return nil, err
		}
		config.Certificates = []tls.Certificate{cert}
	}

	// 如果配置了CA证书，则启用服务器证书验证
	if CACertFilePath != "" {
		config.InsecureSkipVerify = false
	} else {
		config.InsecureSkipVerify = true
		log.Warnf("No CA certificate provided, using insecure TLS connection")
	}

	return config, nil
}
