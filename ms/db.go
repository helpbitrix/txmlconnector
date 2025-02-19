package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/txmlconnector/client/commands"
	log "github.com/sirupsen/logrus"
)

const (
	EnvKeyLogLevel           = "LOG_LEVEL"
	ExportCandleCount        = 0
	asyncInsertWait          = false
	tradeTimeLayout          = "02.01.2006 15:04:05"
	dateLayout               = "02.01.2006" // DD.MM.YYYY
	tableTimeLayout          = "2006-01-02 15:04:05"
	ChCandlesInsertQuery     = "INSERT INTO transaq_candles VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
	ChSecuritiesInsertQuery  = "INSERT INTO transaq_securities VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	ChTradesInsertQuery      = "INSERT INTO transaq_trades VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	ChSecInfoInsertQuery     = "INSERT INTO transaq_securities_info VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	ChQuotesInsert           = "INSERT INTO transaq_quotes VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
	ChQuotesAggregatedInsert = ` INSERT INTO transaq_quotes_aggregated VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	candlesDDL = `CREATE TABLE IF NOT EXISTS transaq_candles (
		date   DateTime('Europe/Moscow'),
		sec_code FixedString(16),
		period UInt8,
		open   Float32,
		close  Float32,
		high   Float32,
		low    Float32,
		volume UInt64
	) ENGINE = ReplacingMergeTree()
	ORDER BY (date, sec_code, period)`

	quotesAggregatedDDL = `CREATE TABLE IF NOT EXISTS transaq_quotes_aggregated (
		timestamp DateTime('Europe/Moscow'),
		secid UInt16,
		board LowCardinality(String),
		sec_code LowCardinality(FixedString(16)),
		price_level Float32,
		side Int8,
		min_volume Int32,
		max_volume Int32,
		avg_volume Float32,
		end_volume Int32,
		updates_count UInt16,
		version UInt32
	) ENGINE = ReplacingMergeTree(version)
	PARTITION BY toYYYYMMDD(timestamp)
	ORDER BY (timestamp, secid, board, sec_code, price_level, side)
	TTL timestamp + INTERVAL 30 DAY;`

	securitiesDDL = `CREATE TABLE IF NOT EXISTS transaq_securities (
		secid   UInt16,
		seccode FixedString(16),
		instrclass String,
		board String,
		market UInt8,
		shortname String,
		decimals UInt8,
		minstep Float32,
		lotsize UInt8,
		point_cost Float32,
		sectype String,
		quotestype UInt8,
		sector String
	) ENGINE = ReplacingMergeTree()
	ORDER BY (seccode, instrclass, board, market, sectype, quotestype)`

	tradesDDL = `CREATE TABLE IF NOT EXISTS transaq_trades (
		time   DateTime('Europe/Moscow'),
		secid   UInt16,
		sec_code LowCardinality(FixedString(16)),
        trade_no Int64,
		board LowCardinality(String),
		price   Float32,
		quantity UInt32,
        buy_sell LowCardinality(FixedString(1)),
        open_interest Int32,
        period LowCardinality(FixedString(1))
	) ENGINE = ReplacingMergeTree()
	ORDER BY (secid, board, sec_code, trade_no, time, buy_sell)`

	securitiesInfoDDL = `CREATE TABLE IF NOT EXISTS transaq_securities_info (
		secid   UInt16,
		sec_name String,
		sec_code FixedString(16),
		market UInt8,
		pname  String,
		mat_date DateTime('Europe/Moscow'),
		clearing_price Float32,
		minprice Float32,
		maxprice Float32,
		buy_deposit Float32,
		sell_deposit Float32,
		bgo_c Float32,
		bgo_nc Float32,
        bgo_buy Float32,
		accruedint Float32,
		coupon_value Float32,
		coupon_date DateTime('Europe/Moscow'),
		coupon_period UInt8,
		facevalue Float32,
		put_call FixedString(1),
		point_cost Float32,
		opt_type FixedString(1),
		lot_volume UInt8,
		isin String,
		regnumber String,
		buybackprice Float32,
		buybackdate DateTime('Europe/Moscow'),
		currencyid String
	) ENGINE = ReplacingMergeTree()
	ORDER BY (sec_code, market, regnumber, isin)`

	quotesDDL = `CREATE TABLE IF NOT EXISTS transaq_quotes (
        time 	 DateTime('Europe/Moscow'),
		secid    UInt16,
		board 	 LowCardinality(String),
		sec_code LowCardinality(FixedString(16)),
    	price    Float32,
		source   LowCardinality(String),
        yield    Int8,
        buy      Int16,
        Sell     Int16,
    ) ENGINE = ReplacingMergeTree()
	ORDER BY (sec_code, board, price, source)
    `
)

const (
	maxRetries = 3
	retryDelay = 1 * time.Second
)

const (
	maxBatchSize = 1000
	flushTimeout = 100 * time.Millisecond
)

type TradesBatcher struct {
	batch     driver.Batch
	count     int
	lastFlush time.Time
	mu        sync.Mutex
	conn      driver.Conn // добавляем соединение
}

var tradesToProcess = make(chan *commands.Trade, 10000)

func NewTradesBatcher() (*TradesBatcher, error) {
	batch, err := connect.PrepareBatch(ctx, ChTradesInsertQuery)
	if err != nil {
		return nil, err
	}
	return &TradesBatcher{
		batch:     batch,
		lastFlush: time.Now(),
		conn:      connect, // Добавляем инициализацию conn
	}, nil
}

func (tb *TradesBatcher) Add(trade *commands.Trade) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	// Проверяем текущий batch
	if tb.batch == nil {
		newBatch, err := tb.createNewBatch()
		if err != nil {
			return fmt.Errorf("cannot create new batch: %v", err)
		}
		tb.batch = newBatch
	}

	tradeTime, _ := time.Parse(tradeTimeLayout, trade.Time)

	// Пробуем добавить в batch
	err := tb.batch.Append(
		fmt.Sprint(tradeTime.Format(tableTimeLayout)),
		trade.SecId,
		trade.SecCode,
		trade.TradeNo,
		trade.Board,
		trade.Price,
		trade.Quantity,
		trade.BuySell,
		trade.OpenInterest,
		trade.Period,
	)

	// Если ошибка с batch - пересоздаем его
	if err != nil {
		log.Warn("Batch error, recreating: ", err)
		tb.batch.Abort() // очищаем старый

		newBatch, err := tb.createNewBatch()
		if err != nil {
			return fmt.Errorf("cannot recreate batch: %v", err)
		}

		tb.batch = newBatch
		tb.count = 0

		// Пробуем добавить снова
		if err := tb.batch.Append(
			fmt.Sprint(tradeTime.Format(tableTimeLayout)),
			trade.SecId,
			trade.SecCode,
			trade.TradeNo,
			trade.Board,
			trade.Price,
			trade.Quantity,
			trade.BuySell,
			trade.OpenInterest,
			trade.Period,
		); err != nil {
			return fmt.Errorf("cannot append to new batch: %v", err)
		}
	}

	tb.count++

	if tb.count >= maxBatchSize || time.Since(tb.lastFlush) >= flushTimeout {
		return tb.flush()
	}

	return nil
}

func (tb *TradesBatcher) createNewBatch() (driver.Batch, error) {
	// Проверяем соединение
	if err := tb.conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("connection check failed: %v", err)
	}

	return tb.conn.PrepareBatch(ctx, ChTradesInsertQuery)
}

func (tb *TradesBatcher) flush() error {
	if tb.count == 0 || tb.batch == nil {
		return nil
	}

	currentBatch := tb.batch

	// Создаем новый batch до отправки старого
	newBatch, err := tb.createNewBatch()
	if err != nil {
		return fmt.Errorf("cannot create new batch during flush: %v", err)
	}

	// Пробуем отправить с retry
	var lastErr error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		if err := currentBatch.Send(); err != nil {
			lastErr = err
			log.Warnf("Failed to send batch (attempt %d/%d): %v", attempt, maxRetries, err)

			if attempt < maxRetries {
				time.Sleep(retryDelay)
				continue
			}

			// Последняя попытка не удалась
			log.Errorf("All attempts to send batch failed: %v", err)
			currentBatch.Abort()

			// Сохраняем новый batch и сбрасываем счетчик
			tb.batch = newBatch
			tb.count = 0
			tb.lastFlush = time.Now()

			return fmt.Errorf("batch send failed after %d attempts: %v", maxRetries, lastErr)
		}

		// Успешная отправка
		tb.batch = newBatch
		tb.count = 0
		tb.lastFlush = time.Now()
		return nil
	}

	return lastErr // не должны сюда попасть
}

func processTrades(batcher *TradesBatcher) {
	for trade := range tradesToProcess {
		atomic.AddInt64(&metrics.TradesInQueue, 1)

		if err := batcher.Add(trade); err != nil {
			log.Error(err)
			atomic.AddInt64(&metrics.TradesErrorCount, 1)
		} else {
			atomic.AddInt64(&metrics.TradesInserted, 1)
		}

		atomic.AddInt64(&metrics.TradesInQueue, -1)
	}
}

func insertQuote(quote *commands.Quote, eventTime *time.Time) error {
	return connect.AsyncInsert(ctx, ChQuotesInsert, asyncInsertWait,
		fmt.Sprint(eventTime.Format(tableTimeLayout)),
		quote.SecId,
		quote.Board,
		quote.SecCode,
		quote.Price,
		quote.Source,
		quote.Yield,
		quote.Buy,
		quote.Sell,
	)
}

func insertSecInfo(secInfo *commands.SecInfo) error {
	matDate, _ := time.Parse(dateLayout, secInfo.MatDate)
	couponDate, _ := time.Parse(dateLayout, secInfo.CouponDate)
	buybackDate, _ := time.Parse(dateLayout, secInfo.BuybackDate)
	return connect.AsyncInsert(ctx, ChSecInfoInsertQuery, asyncInsertWait,
		secInfo.SecId,
		secInfo.SecName,
		secInfo.SecCode,
		secInfo.Market,
		secInfo.PName,
		fmt.Sprint(matDate.Format(tableTimeLayout)),
		secInfo.ClearingPrice,
		secInfo.MinPrice,
		secInfo.MaxPrice,
		secInfo.BuyDeposit,
		secInfo.SellDeposit,
		secInfo.BgoC,
		secInfo.BgoNc,
		secInfo.BgoBuy,
		secInfo.AccruedInt,
		secInfo.CouponValue,
		fmt.Sprint(couponDate.Format(tableTimeLayout)),
		secInfo.CouponPeriod,
		secInfo.CouponPeriod,
		secInfo.FaceValue,
		secInfo.PutCall,
		secInfo.PointCost,
		secInfo.OptType,
		secInfo.LotVolume,
		secInfo.Isin,
		secInfo.RegNumber,
		secInfo.BuybackPrice,
		fmt.Sprint(buybackDate.Format(tableTimeLayout)),
		secInfo.CurrencyId,
	)
}
