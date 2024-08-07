// Copyright (c) 2024 ENRICO RUBBOLI - enrico@rbblab.com
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/spf13/viper"
	"go.uber.org/zap"
)

var (
	logger *zap.SugaredLogger
	cfg    *Config
)

type Config struct {
	APIBaseURL string
	APITimeout time.Duration
}

// Tip represents the structure of the response from /chain/tip
type Tip struct {
	BlockHeight int    `json:"block_height"`
	BlockID     string `json:"block_id"`
}

// BlockHeader represents the structure of the response from /chain/{block_hash}/header
type BlockHeader struct {
	ConsensusData struct {
		Target string `json:"target"`
	} `json:"consensus_data"`
	MerkleRoot      string `json:"merkle_root"`
	PreviousBlockID string `json:"previous_block_id"`
	Timestamp       struct {
		Timestamp int64 `json:"timestamp"`
	} `json:"timestamp"`
	WitnessMerkleRoot string `json:"witness_merkle_root"`
	Hash              string `json:"hash"`
}

func fetchData(endpoint string, target interface{}) error {
	u := fmt.Sprintf("%s%s", cfg.APIBaseURL, endpoint)
	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	client := &http.Client{
		Timeout: cfg.APITimeout,
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to fetch data: %s", resp.Status)
	}

	return json.NewDecoder(resp.Body).Decode(target)
}

func fetchTip() (*Tip, error) {
	var tip Tip
	err := fetchData("/api/v2/chain/tip", &tip)
	if err != nil {
		return nil, err
	}
	return &tip, nil
}

func fetchBlockHash(height int) (string, error) {
	var hash string
	err := fetchData(fmt.Sprintf("/api/v2/chain/%d", height), &hash)
	if err != nil {
		return "", err
	}
	return hash, nil
}

func fetchBlockHeader(blockHash string) (*BlockHeader, error) {
	var header BlockHeader
	err := fetchData(fmt.Sprintf("/api/v2/block/%s/header", blockHash), &header)
	if err != nil {
		return nil, err
	}
	return &header, nil
}

func main() {
	initConfig()

	tip, err := fetchTip()
	if err != nil {
		logger.Fatalf("Error fetching tip: %v", err)
	}

	fmt.Printf("%-8s | %-20s | %-64s\n", "Height", "Timestamp", "Hash")
	fmt.Println(strings.Repeat("-", 96))
	for height := 1; height <= tip.BlockHeight; height++ {
		blockHash, err := fetchBlockHash(height)
		if err != nil {
			logger.Errorf("Error fetching block hash for height %d: %v", height, err)
			continue
		}

		header, err := fetchBlockHeader(blockHash)
		if err != nil {
			logger.Errorf("Error fetching block header for height %d: %v", height, err)
			continue
		}

		fmt.Printf("%-8d | %-20s | %-64s\n", height, time.Unix(header.Timestamp.Timestamp, 0).Format(time.RFC3339), blockHash)
		time.Sleep(1 * time.Second) // Sleep for 1 second to avoid overwhelming the API server
	}
}

func initConfig() {
	viper.SetConfigName("config")
	viper.AddConfigPath(".")
	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err != nil {
		log.Fatalf("Failed to read config file: %v", err)
	}

	cfg = &Config{
		APIBaseURL: viper.GetString("api_base_url"),
		APITimeout: viper.GetDuration("api_timeout"),
	}

	if cfg.APIBaseURL == "" {
		cfg.APIBaseURL = "https://api-server.mintlayer.org"
	}

	if cfg.APITimeout == 0 {
		cfg.APITimeout = 30 * time.Second
	}

	logger, _ = zap.NewProduction()
	defer logger.Sync()
}
