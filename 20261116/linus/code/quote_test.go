package main

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestParseTicker(t *testing.T) {
	body := []byte(`{"code":"0","msg":"","data":[{"instId":"BTC-USDT","last":"64139.2","vol24h":"8599.80067709","ts":"1794834726267"}]}`)
	got, err := parseTicker(body)
	if err != nil {
		t.Fatal(err)
	}
	if got.Symbol != "BTC-USDT" || got.Price != "64139.2" || got.Volume != "8599.80067709" {
		t.Fatalf("%+v", got)
	}
	want := time.UnixMilli(1794834726267).In(shanghai).Format("2006-01-02 15:04:05")
	if got.Ts != want {
		t.Fatalf("ts=%s want %s", got.Ts, want)
	}
}

func TestParseTickerRejects(t *testing.T) {
	if _, err := parseTicker([]byte(`{"code":"51001","msg":"instId error","data":[]}`)); err == nil {
		t.Fatal("expected error")
	}
	if _, err := parseTicker([]byte(`{"code":"0","data":[]}`)); err == nil {
		t.Fatal("expected error")
	}
}

func TestFetchBTC(t *testing.T) {
	if os.Getenv("LIVE") == "" {
		t.Skip()
	}
	tk, err := fetchTicker(context.Background(), "BTC-USDT")
	if err != nil {
		t.Fatal(err)
	}
	if tk.Symbol != "BTC-USDT" || tk.Price == "" || tk.Ts == "" {
		t.Fatalf("%+v", tk)
	}
}

func TestTrimDecimal(t *testing.T) {
	if got := trimDecimal("1798.63000000"); got != "1798.63" {
		t.Fatal(got)
	}
	if got := trimDecimal("0.00000000"); got != "0" {
		t.Fatal(got)
	}
}
