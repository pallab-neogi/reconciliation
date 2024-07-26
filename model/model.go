package model

import (
	"fmt"
	"time"

	"github.com/spf13/viper"
)

// Configuration structure
type Config struct {
	Azure struct {
		AccountName   string
		AccountKey    string
		ShareName     string
		DirectoryPath map[string]string
		FileName      string
	}
	Kafka struct {
		Broker  string
		Topic   string
		GroupID string
	}
	CSV struct {
		ChunkSize int
	}
	Retry struct {
		MaxRetries    uint64
		RetryInterval time.Duration
	}
	Postgres struct {
		Server   string
		User     string
		Password string
	}
	Fabric struct {
		MspID        string
		CryptoPath   string
		CertPath     string
		KeyPath      string
		TlsCertPath  string
		PeerEndpoint string
		GatewayPeer  string
	}
	Dlt struct {
		ChannelID      string
		ChaincodeID    string
		User           string
		Org            string
		ConfigFilePath string
	}
}

func InitConfig(configname string) (*Config, error) {
	viper.SetConfigName(configname)
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")

	if err := viper.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config Config
	if err := viper.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return &config, nil
}
