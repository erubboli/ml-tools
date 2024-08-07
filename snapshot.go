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

// NOTE: this is not production ready.

// Crontab:
// 0 0 * * * /path/to/your/go/executable >> /path/to/your/logfile 2>&1

/**

you can freeze the chain at a specific HEIGHT with the following code:
```
--- a/chainstate/src/detail/mod.rs
+++ b/chainstate/src/detail/mod.rs
@@ -20,6 +20,7 @@ mod info;
 mod median_time;
 mod orphan_blocks;

+
 pub mod ban_score;
 pub mod block_checking;
 pub mod block_invalidation;
@@ -27,6 +28,7 @@ pub mod bootstrap;
 pub mod query;
 pub mod tx_verification_strategy;

+use std::env;
 use std::{collections::VecDeque, sync::Arc};

 use itertools::Itertools;
@@ -345,6 +347,14 @@ impl<S: BlockchainStorage, V: TransactionVerificationStrategy> Chainstate<S, V>
     ) -> Result<bool, BlockIntegrationError> {
         let mut block_status = BlockStatus::new();

+        let max_block_height = env::var("MAX_BLOCK_HEIGHT")
+             .map(|v| v.parse::<u64>().expect("Failed to parse MAX_BLOCK_HEIGHT"))
+             .unwrap_or(0);
+
+        if block_index.block_height() > BlockHeight::new(max_block_height) { // 06-09
+             return Ok(false);
+        }
+
         chainstate_ref
             .check_block(block)
             .map_err(BlockError::CheckBlockFailed)
```
and run the node as:

```
MAX_BLOCK_HEIGHT=1000 cargo run --bin node-daemon
```

NOTE: when you do that you will disconnect yourself from the network cause everyone will send you invalid blocks at some point, 
      when needed you can recover the situation removing peers database in the datadir_

**/

package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	_ "github.com/mattn/go-sqlite3"
)

var (
	logger *zap.SugaredLogger
	cfg    *Config
)

type Config struct {
	NodeAPIBaseURL     string
	ExplorerAPIBaseURL string
	DatabasePath       string
	WorkerPoolSize     int
	APITimeout         time.Duration
}

type APIClient struct {
	httpClient     *http.Client
	nodeBaseURL    string
	explorerBaseURL string
}

type Pool struct {
	CostPerBlock            float64 `json:"cost_per_block"`
	DecommissionDestination string  `json:"decommission_destination"`
	MarginRatioPerThousand  string  `json:"margin_ratio_per_thousand"`
	PoolID                  string  `json:"pool_id"`
	StakerBalance           float64 `json:"staker_balance"`
	VRFPublicKey            string  `json:"vrf_public_key"`
	DelegationsAmountAtoms  int64   `json:"delegations_amount_atoms"`
	DelegationsAmount       float64 `json:"delegations_amount"`
	DelegationsCount        int     `json:"delegations_count"`
	MarginRatio             float64 `json:"margin_ratio"`
	Balance                 float64 `json:"balance"`
	EffectivePoolBalance    float64 `json:"effective_pool_balance"`
}

type Delegation struct {
	Balance struct {
		Atoms   string `json:"atoms"`
		Decimal string `json:"decimal"`
	} `json:"balance"`
	DelegationID     string `json:"delegation_id"`
	NextNonce        int    `json:"next_nonce"`
	SpendDestination string `json:"spend_destination"`
}

type Tip struct {
	BlockHeight int    `json:"block_height"`
	BlockID     string `json:"block_id"`
}

func NewAPIClient(cfg *Config) *APIClient {
	return &APIClient{
		httpClient: &http.Client{
			Timeout: cfg.APITimeout,
		},
		nodeBaseURL:    cfg.NodeAPIBaseURL,
		explorerBaseURL: cfg.ExplorerAPIBaseURL,
	}
}

func (c *APIClient) FetchTip(ctx context.Context) (int, error) {
	url := fmt.Sprintf("%s/api/v2/chain/tip", c.nodeBaseURL)
	var tip Tip
	err := c.fetchJSON(ctx, url, &tip)
	if err != nil {
		return 0, fmt.Errorf("failed to fetch tip: %w", err)
	}
	return tip.BlockHeight, nil
}

func (c *APIClient) FetchPoolData(ctx context.Context) ([]Pool, error) {
	url := fmt.Sprintf("%s/api/pool/list?withBalance=true", c.explorerBaseURL)
	var pools []Pool
	err := c.fetchJSON(ctx, url, &pools)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch pools: %w", err)
	}
	return pools, nil
}

func (c *APIClient) FetchDelegationData(ctx context.Context, poolID string) ([]Delegation, error) {
	url := fmt.Sprintf("%s/api/v2/pool/%s/delegations?offset=0", c.nodeBaseURL, poolID)
	var delegations []Delegation
	err := c.fetchJSON(ctx, url, &delegations)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch delegations for pool %s: %w", poolID, err)
	}
	return delegations, nil
}

func (c *APIClient) fetchJSON(ctx context.Context, url string, target interface{}) error {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return json.NewDecoder(resp.Body).Decode(target)
}

func isUniqueConstraintError(err error) bool {
	return strings.Contains(err.Error(), "UNIQUE constraint failed")
}

func savePoolData(db *sql.DB, pools []Pool, date string) error {
	stmt, err := db.Prepare("INSERT INTO pools(date, pool_id, staker_balance, decommission_destination) VALUES (?, ?, ?, ?)")
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, pool := range pools {
		if pool.Balance == 0 {
			logger.Infof("Pool %v decommissioned", pool.PoolID)
			continue
		}
		_, err = stmt.Exec(date, pool.PoolID, pool.StakerBalance, pool.DecommissionDestination)
		if err != nil && !isUniqueConstraintError(err) {
			return err
		}
	}

	return nil
}

func saveDelegationData(db *sql.DB, delegations []Delegation, date string) error {
	stmt, err := db.Prepare("INSERT INTO delegations(date, delegation_id, balance, spend_destination) VALUES (?, ?, ?, ?)")
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, delegation := range delegations {
		balance, err := strconv.ParseFloat(delegation.Balance.Decimal, 64)
		if err != nil {
			return err
		}
		if !(balance > 0) {
			continue
		}
		_, err = stmt.Exec(date, delegation.DelegationID, balance, delegation.SpendDestination)
		if err != nil && !isUniqueConstraintError(err) {
			return err
		}
	}

	return nil
}

func saveAggregatedData(db *sql.DB, date string, data map[string]float64) error {
	stmt, err := db.Prepare("INSERT INTO aggregated_data(date, address, amount) VALUES (?, ?, ?) ON CONFLICT(date, address) DO UPDATE SET amount=excluded.amount")
	if err != nil {
		return err
	}
	defer stmt.Close()

	for address, amount := range data {
		if amount < 100 {
			continue
		}
		_, err = stmt.Exec(date, address, amount)
		if err != nil {
			return err
		}
	}

	return nil
}

func aggregateData(db *sql.DB, date string) (map[string]float64, error) {
	addressSums := make(map[string]float64)

	query := `
    SELECT address, SUM(balance) as total_balance FROM (
        SELECT spend_destination as address, balance
        FROM delegations
        WHERE date = ? AND balance > 100
        UNION ALL
        SELECT decommission_destination as address, staker_balance as balance
        FROM pools
        WHERE date = ?
    ) combined
    GROUP BY address
    ORDER BY total_balance
    `

	rows, err := db.Query(query, date, date)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var address string
		var balance float64
		if err := rows.Scan(&address, &balance); err != nil {
			return nil, err
		}
		addressSums[address] = balance
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return addressSums, nil
}

func generateCSV(date string, data map[string]float64) error {
	file, err := os.Create(fmt.Sprintf("snapshots/snapshot_data_%s.csv", date))
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write CSV header
	err = writer.Write([]string{"DAY", "address", "amount"})
	if err != nil {
		return err
	}

	// Write data
	for address, amount := range data {
		err = writer.Write([]string{date, address, fmt.Sprintf("%.8f", amount)})
		if err != nil {
			return err
		}
	}

	return nil
}

func printStats(data map[string]float64, date string, tip int, pools int, delegations int64) {
	var total float64
	for _, amount := range data {
		total += amount
	}

	logger.Infof("TIP: %v - DATE: %v", tip, date)
	logger.Infof("POOLS: %v - DELEGATIONS: %v", pools, delegations)
	logger.Infof("Total ML in stake at %v: %.8f", date, total)
	logger.Infof("Number of unique addresses: %v", len(data))
}

func processPools(ctx context.Context, db *sql.DB, apiClient *APIClient, pools []Pool, date string) (int64, error) {
	var totalDelegations int64
	poolChan := make(chan Pool)
	resultChan := make(chan int)
	errorChan := make(chan error)

	var wg sync.WaitGroup
	for i := 0; i < cfg.WorkerPoolSize; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for pool := range poolChan {
				delegations, err := apiClient.FetchDelegationData(ctx, pool.PoolID)
				if err != nil {
					errorChan <- err
					continue
				}

				err = saveDelegationData(db, delegations, date)
				if err != nil {
					errorChan <- err
					continue
				}

				resultChan <- len(delegations)
			}
		}()
	}

	go func() {
		for _, pool := range pools {
			if pool.Balance == 0 {
				continue
			}
			poolChan <- pool
		}
		close(poolChan)
	}()

	go func() {
		wg.Wait()
		close(resultChan)
		close(errorChan)
	}()

	for {
		select {
		case count, ok := <-resultChan:
			if !ok {
				return totalDelegations, nil
			}
			totalDelegations += int64(count)
		case err := <-errorChan:
			return totalDelegations, err
		case <-ctx.Done():
			return totalDelegations, ctx.Err()
		}
	}
}

func run(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	apiClient := NewAPIClient(cfg)

	db, err := sql.Open("sqlite3", cfg.DatabasePath)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	date := time.Now().Format("2006-01-02")
	if len(args) > 0 {
		date = args[0]
	}

	tip, err := apiClient.FetchTip(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch tip: %w", err)
	}

	pools, err := apiClient.FetchPoolData(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch pool data: %w", err)
	}

	err = savePoolData(db, pools, date)
	if err != nil {
		return fmt.Errorf("failed to save pool data: %w", err)
	}

	totDelegations, err := processPools(ctx, db, apiClient, pools, date)
	if err != nil {
		return fmt.Errorf("failed to process pools: %w", err)
	}

	addressSums, err := aggregateData(db, date)
	if err != nil {
		return fmt.Errorf("failed to aggregate data: %w", err)
	}

	err = saveAggregatedData(db, date, addressSums)
	if err != nil {
		return fmt.Errorf("failed to save aggregated data: %w", err)
	}

	err = generateCSV(date, addressSums)
	if err != nil {
		return fmt.Errorf("failed to generate CSV: %w", err)
	}

	printStats(addressSums, date, tip, len(pools), totDelegations)
	return nil
}

func main() {
	rootCmd := &cobra.Command{
		Use:   "stake-snapshot [date]",
		Short: "Generate a stake snapshot for Mintlayer",
		RunE:  run,
	}

	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().String("config", "", "config file (default is ./config.yaml)")

	if err := rootCmd.Execute(); err != nil {
		logger.Fatal(err)
	}
}

func initConfig() {
	viper.SetConfigName("config")
	viper.AddConfigPath(".")
	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err != nil {
		logger.Fatalf("Failed to read config file: %v", err)
	}

	cfg = &Config{
		NodeAPIBaseURL:     viper.GetString("node_api_base_url"),
		ExplorerAPIBaseURL: viper.GetString("explorer_api_base_url"),
		DatabasePath:       viper.GetString("database_path"),
		WorkerPoolSize:     viper.GetInt("worker_pool_size"),
		APITimeout:         viper.GetDuration("api_timeout"),
	}

	logger, _ = zap.NewProduction()
	defer logger.Sync()
}