package main

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

//go:embed schema.sql
var schemaSQL string

// 从 OKX 拉 BTC-USDT、ETH-USDT 最新价，写入 Doris，页面上查看。
// 默认连本机 Doris：root 无密码，端口 9030。
//
//	go run .
//
// 浏览器打开 http://127.0.0.1:8080
func main() {
	host := env("DORIS_HOST", "127.0.0.1")
	port := env("DORIS_PORT", "9030")
	user := env("DORIS_USER", "root")
	password := env("DORIS_PASSWORD", "")
	dbName := env("DORIS_DB", "market")
	addr := env("ADDR", ":8080")
	if !dbNameRe.MatchString(dbName) {
		log.Fatalf("DORIS_DB 不合法: %s", dbName)
	}

	admin, err := openDB(dsn(user, password, host, port, ""))
	if err != nil {
		log.Fatal(err)
	}
	defer admin.Close()

	pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	err = admin.PingContext(pingCtx)
	pingCancel()
	if err != nil {
		log.Fatalf("连不上 Doris %s:%s ：%v", host, port, err)
	}

	schemaCtx, schemaCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer schemaCancel()
	if _, err := admin.ExecContext(schemaCtx, "CREATE DATABASE IF NOT EXISTS "+dbName); err != nil {
		log.Fatalf("建库失败: %v", err)
	}

	db, err := openDB(dsn(user, password, host, port, dbName))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(schemaCtx, schemaSQL); err != nil {
		log.Fatalf("建表失败: %v", err)
	}

	go poll(db)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(page))
	})
	mux.HandleFunc("GET /api/quotes", func(w http.ResponseWriter, r *http.Request) {
		rows, err := latestQuotes(r.Context(), db)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, rows)
	})
	mux.HandleFunc("GET /api/history", func(w http.ResponseWriter, r *http.Request) {
		symbol := r.URL.Query().Get("symbol")
		if !knownSymbol(symbol) {
			writeErr(w, http.StatusBadRequest, fmt.Errorf("symbol 只支持 %v", symbols))
			return
		}
		limit := 30
		if raw := r.URL.Query().Get("limit"); raw != "" {
			n, err := strconv.Atoi(raw)
			if err != nil || n < 1 || n > 200 {
				writeErr(w, http.StatusBadRequest, fmt.Errorf("limit 要在 1 到 200"))
				return
			}
			limit = n
		}
		rows, err := history(r.Context(), db, symbol, limit)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, rows)
	})

	log.Printf("行情服务 http://127.0.0.1%s  库 %s", addr, dbName)
	server := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Fatal(server.ListenAndServe())
}

func poll(db *sql.DB) {
	run := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := ingest(ctx, db); err != nil {
			log.Printf("采集失败: %v", err)
		}
	}
	run()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		run()
	}
}

func openDB(dsn string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(time.Minute)
	return db, nil
}

func dsn(user, password, host, port, database string) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?interpolateParams=true&charset=utf8mb4&timeout=5s&readTimeout=30s&writeTimeout=30s",
		user, password, host, port, database)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, err error) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

var dbNameRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

const page = `<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>行情</title>
<style>
  body { font: 15px/1.5 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 32px auto; max-width: 800px; color: #1c1c1c; background: #f6f7f9; padding: 0 16px; }
  h1 { font-size: 22px; font-weight: 600; margin-bottom: 4px; }
  .sub { color: #666; margin-top: 0; }
  #cards { display: flex; gap: 16px; flex-wrap: wrap; }
  section { flex: 1; min-width: 220px; background: #fff; border-radius: 12px; padding: 16px 20px; }
  h2 { font-size: 14px; margin: 0; color: #666; font-weight: 500; }
  .price { font-size: 32px; margin: 8px 0; font-variant-numeric: tabular-nums; }
  .meta { color: #666; margin: 0; }
  table { width: 100%; border-collapse: collapse; background: #fff; border-radius: 12px; overflow: hidden; }
  th, td { text-align: left; padding: 8px 12px; border-bottom: 1px solid #eee; font-variant-numeric: tabular-nums; }
  th { color: #666; font-weight: 500; }
  #note { color: #a30; min-height: 1.2em; }
  h3 { font-size: 16px; }
</style>
</head>
<body>
  <h1>BTC / ETH</h1>
  <p class="sub">OKX 最新价，每 5 秒写入 Doris</p>
  <p id="note"></p>
  <div id="cards"></div>
  <h3>最近写入</h3>
  <table>
    <thead><tr><th>交易对</th><th>时间</th><th>价格</th><th>24h 量</th></tr></thead>
    <tbody id="rows"></tbody>
  </table>
<script>
const symbols = ["BTC-USDT", "ETH-USDT"];

function card(q) {
  const el = document.createElement("section");
  const name = document.createElement("h2");
  name.textContent = q.symbol;
  const price = document.createElement("p");
  price.className = "price";
  price.textContent = q.price;
  const meta = document.createElement("p");
  meta.className = "meta";
  meta.textContent = q.ts + " · 24h 量 " + q.volume;
  el.append(name, price, meta);
  return el;
}

function addRow(tk) {
  const tr = document.createElement("tr");
  for (const text of [tk.symbol, tk.ts, tk.price, tk.volume]) {
    const td = document.createElement("td");
    td.textContent = text;
    tr.append(td);
  }
  return tr;
}

async function load() {
  const note = document.querySelector("#note");
  try {
    const res = await fetch("/api/quotes");
    const quotes = await res.json();
    if (!res.ok) {
      note.textContent = quotes.error || "查询失败";
      return;
    }
    note.textContent = quotes.length ? "" : "还没有数据，稍等几秒。";
    const cards = document.querySelector("#cards");
    cards.replaceChildren(...quotes.map(card));

    const lists = await Promise.all(symbols.map(async (symbol) => {
      const hist = await fetch("/api/history?symbol=" + encodeURIComponent(symbol) + "&limit=8");
      if (!hist.ok) return [];
      return hist.json();
    }));
    document.querySelector("#rows").replaceChildren(...lists.flat().map(addRow));
  } catch (err) {
    note.textContent = "请求失败";
  }
}

load();
setInterval(load, 3000);
</script>
</body>
</html>
`
