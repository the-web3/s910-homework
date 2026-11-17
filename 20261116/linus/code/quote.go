package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// 只采集这两个现货交易对。
var symbols = []string{"BTC-USDT", "ETH-USDT"}

var shanghai = time.FixedZone("CST", 8*3600)

var httpClient = &http.Client{Timeout: 8 * time.Second}

type tick struct {
	Symbol string `json:"symbol"`
	Ts     string `json:"ts"`
	Price  string `json:"price"`
	Volume string `json:"volume"`
}

type okxResp struct {
	Code string `json:"code"`
	Msg  string `json:"msg"`
	Data []struct {
		InstID string `json:"instId"`
		Last   string `json:"last"`
		Vol24h string `json:"vol24h"`
		Ts     string `json:"ts"`
	} `json:"data"`
}

func parseTicker(body []byte) (tick, error) {
	var resp okxResp
	if err := json.Unmarshal(body, &resp); err != nil {
		return tick{}, err
	}
	if resp.Code != "0" {
		if resp.Msg == "" {
			resp.Msg = resp.Code
		}
		return tick{}, fmt.Errorf("okx: %s", resp.Msg)
	}
	if len(resp.Data) == 0 {
		return tick{}, fmt.Errorf("okx: 没有行情")
	}
	item := resp.Data[0]
	ms, err := strconv.ParseInt(item.Ts, 10, 64)
	if err != nil {
		return tick{}, fmt.Errorf("okx 时间: %w", err)
	}
	if item.InstID == "" || item.Last == "" {
		return tick{}, fmt.Errorf("okx: 行情字段不完整")
	}
	if item.Vol24h == "" {
		item.Vol24h = "0"
	}
	return tick{
		Symbol: item.InstID,
		Ts:     time.UnixMilli(ms).In(shanghai).Format("2006-01-02 15:04:05"),
		Price:  item.Last,
		Volume: item.Vol24h,
	}, nil
}

func fetchTicker(ctx context.Context, instID string) (tick, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://www.okx.com/api/v5/market/ticker?instId="+instID, nil)
	if err != nil {
		return tick{}, err
	}
	res, err := httpClient.Do(req)
	if err != nil {
		return tick{}, err
	}
	defer res.Body.Close()
	body, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	if err != nil {
		return tick{}, err
	}
	if res.StatusCode != http.StatusOK {
		return tick{}, fmt.Errorf("okx http %s", res.Status)
	}
	tk, err := parseTicker(body)
	if err != nil {
		return tick{}, err
	}
	if tk.Symbol != instID {
		return tick{}, fmt.Errorf("okx 返回了 %s，要的是 %s", tk.Symbol, instID)
	}
	return tk, nil
}

func ingest(ctx context.Context, db *sql.DB) error {
	var errs []error
	for _, symbol := range symbols {
		tk, err := fetchTicker(ctx, symbol)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", symbol, err))
			continue
		}
		if err := saveTick(ctx, db, tk); err != nil {
			errs = append(errs, fmt.Errorf("写入 %s: %w", symbol, err))
			continue
		}
		log.Printf("写入 %s %s @ %s", tk.Symbol, tk.Price, tk.Ts)
	}
	return errors.Join(errs...)
}

func saveTick(ctx context.Context, db *sql.DB, tk tick) error {
	_, err := db.ExecContext(ctx, `
		INSERT INTO ticks (symbol, ts, price, volume)
		VALUES (?, ?, ?, ?)`,
		tk.Symbol, tk.Ts, tk.Price, tk.Volume)
	return err
}

func latestQuotes(ctx context.Context, db *sql.DB) ([]tick, error) {
	out := []tick{}
	for _, symbol := range symbols {
		row, err := one(ctx, db, `
			SELECT symbol, ts, price, volume
			FROM ticks
			WHERE symbol = ?
			ORDER BY ts DESC
			LIMIT 1`, symbol)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, nil
}

func history(ctx context.Context, db *sql.DB, symbol string, limit int) ([]tick, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT symbol, ts, price, volume
		FROM ticks
		WHERE symbol = ?
		ORDER BY ts DESC
		LIMIT `+strconv.Itoa(limit), symbol)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []tick{}
	for rows.Next() {
		var tk tick
		if err := rows.Scan(&tk.Symbol, &tk.Ts, &tk.Price, &tk.Volume); err != nil {
			return nil, err
		}
		tk.Price = trimDecimal(tk.Price)
		tk.Volume = trimDecimal(tk.Volume)
		out = append(out, tk)
	}
	return out, rows.Err()
}

func one(ctx context.Context, db *sql.DB, query, symbol string) (tick, error) {
	var tk tick
	err := db.QueryRowContext(ctx, query, symbol).Scan(&tk.Symbol, &tk.Ts, &tk.Price, &tk.Volume)
	if err != nil {
		return tick{}, err
	}
	tk.Price = trimDecimal(tk.Price)
	tk.Volume = trimDecimal(tk.Volume)
	return tk, nil
}

func trimDecimal(s string) string {
	if !strings.Contains(s, ".") {
		return s
	}
	s = strings.TrimRight(s, "0")
	return strings.TrimRight(s, ".")
}

func knownSymbol(symbol string) bool {
	for _, item := range symbols {
		if item == symbol {
			return true
		}
	}
	return false
}
