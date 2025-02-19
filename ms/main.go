package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path"
	"runtime"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/joho/godotenv"
	tcClient "github.com/kmlebedev/txmlconnector/client"
	"github.com/kmlebedev/txmlconnector/client/commands"
	log "github.com/sirupsen/logrus"
)

var (
	ctx                  = context.Background()
	lvl                  log.Level
	tc                   *tcClient.TCClient
	connect              driver.Conn
	quotations           = []commands.SubSecurity{}
	positions            = commands.Positions{}
	quotationCandles     = make(map[int]commands.Candle)
	dataCandleCount      int
	dataCandleCountLock  = sync.RWMutex{}
	isAllTradesPositions = false
	allTrades            = commands.SubAllTrades{}
	getSecuritiesInfo    = []int{}
	exportSecInfoNames   = []string{}
	isRealtime           = false
	stopChan             = make(chan struct{})
	metrics              = &Metrics{}
)

type Metrics struct {
	LastSuccessfulUpdate time.Time
	ErrorCount           int
	ReconnectCount       int
	ProcessedTicks       int64
	ProcessedCandles     int64
	LastError            string
	LastErrorTime        time.Time
	TradesInQueue        int64
	TradesInserted       int64
	TradesErrorCount     int64
	LastTradeReceived    time.Time
}

func setupLogging() error {
	// Создаем директорию для логов если её нет
	if err := os.MkdirAll("logs", 0755); err != nil {
		return fmt.Errorf("failed to create logs directory: %v", err)
	}

	// Создаем файл лога с датой
	currentTime := time.Now()
	logFileName := fmt.Sprintf("logs/app_%s.log", currentTime.Format("2006-01-02"))
	logFile, err := os.OpenFile(logFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return fmt.Errorf("failed to open log file: %v", err)
	}

	// Настраиваем логгер
	log.SetOutput(io.MultiWriter(os.Stdout, logFile))
	log.SetFormatter(&log.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02 15:04:05",
		CallerPrettyfier: func(f *runtime.Frame) (string, string) {
			filename := path.Base(f.File)
			return fmt.Sprintf("%s()", f.Function), fmt.Sprintf("%s:%d", filename, f.Line)
		},
	})
	log.SetReportCaller(true)

	return nil
}

func ensureClickHouseConnection() error {
	for i := 0; i < 3; i++ {
		if err := connect.Ping(ctx); err != nil {
			log.Warnf("ClickHouse ping failed, attempt %d: %v", i+1, err)
			time.Sleep(time.Second * 2)
			continue
		}
		return nil
	}
	return fmt.Errorf("failed to connect to ClickHouse after 3 attempts")
}

func monitorClickHouse() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-stopChan:
			return
		case <-ticker.C:
			if err := ensureClickHouseConnection(); err != nil {
				log.Errorf("Lost connection to ClickHouse: %v", err)
				metrics.ErrorCount++
				metrics.LastError = fmt.Sprintf("ClickHouse connection lost: %v", err)
				metrics.LastErrorTime = time.Now()
			}
		}
	}
}

func monitorSystem() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-stopChan:
			return
		case <-ticker.C:
			var m runtime.MemStats
			runtime.ReadMemStats(&m)

			log.Infof("System stats - Goroutines: %d, Heap: %d MB, System: %d MB",
				runtime.NumGoroutine(),
				m.Alloc/1024/1024,
				m.Sys/1024/1024,
			)
		}
	}
}

func maintainConnection() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-stopChan:
			return
		case <-ticker.C:
			if tc.Data.ServerStatus.Connected != "true" {
				log.Warn("Connection lost, attempting to reconnect...")
				metrics.ReconnectCount++

				if err := tc.Connect(); err != nil {
					log.Errorf("Reconnect failed: %v", err)
					metrics.ErrorCount++
					metrics.LastError = fmt.Sprintf("Reconnect failed: %v", err)
					metrics.LastErrorTime = time.Now()
					continue
				}

				if err := resubscribeAll(); err != nil {
					log.Errorf("Resubscription failed: %v", err)
					metrics.ErrorCount++
				}
			}
		}
	}
}

func resubscribeAll() error {
	cmd := commands.Command{
		Id:         "subscribe",
		Client:     "TCTEST",
		AllTrades:  allTrades,
		Quotations: quotations,
		Quotes:     quotations, // Важно добавить
	}
	return tc.SendCommand(cmd)
}

func reportMetrics() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-stopChan:
			return
		case <-ticker.C:
			// Убрал логи
		}
	}
}

func init() {
	if err := setupLogging(); err != nil {
		log.Fatalf("Failed to setup logging: %v", err)
	}

	var err error
	if err := godotenv.Load(); err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}

	if lvl, err = log.ParseLevel(os.Getenv(EnvKeyLogLevel)); err == nil {
		log.SetLevel(lvl)
	}

	if tc, err = tcClient.NewTCClient(); err != nil {
		log.Fatal(err)
	}
}

func main() {
	// Отлов паник
	defer func() {
		if r := recover(); r != nil {
			log.Errorf("Recovered from panic in main: %v\nStack trace:\n%s", r, debug.Stack())
			metrics.ErrorCount++
			metrics.LastError = fmt.Sprintf("Panic in main: %v", r)
			metrics.LastErrorTime = time.Now()
		}
	}()

	// ДОБАВИТЬ ЭТО:
	clickhouseUrl := "tcp://127.0.0.1:9000"
	if chUrl := os.Getenv("CLICKHOUSE_URL"); chUrl != "" {
		clickhouseUrl = chUrl
	}

	clickhouseOptions, _ := clickhouse.ParseDSN(clickhouseUrl)

	var err error
	maxAttempts := 10
	for i := 0; i < maxAttempts; i++ {
		log.Infof("Попытка %d из %d: подключение к %s", i+1, maxAttempts, clickhouseUrl)
		if connect, err = clickhouse.Open(clickhouseOptions); err != nil {
			log.Errorf("Ошибка подключения: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}
		if err := connect.Ping(ctx); err != nil {
			log.Errorf("Ошибка ping: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}
		log.Info("Успешное подключение к ClickHouse")
		break
	}
	if err != nil {
		log.Fatalf("Не удалось подключиться к ClickHouse после %d попыток", maxAttempts)
	}

	// Теперь проверяем таблицы
	log.Info("Проверка таблиц в ClickHouse...")
	for _, ddl := range []string{candlesDDL, securitiesDDL, securitiesInfoDDL, tradesDDL, quotesDDL, quotesAggregatedDDL} {
		if err := connect.Exec(ctx, ddl); err != nil {
			log.Fatalf("Ошибка создания таблиц в ClickHouse: %v", err)
		}
	}
	log.Info("Таблицы в ClickHouse проверены")
	// Создаем батчеры
	for i := 0; i < 4; i++ {
		batcher, err := NewTradesBatcher()
		if err != nil {
			log.Fatalf("Failed to create trade batcher: %v", err)
		}
		go processTrades(batcher)
	}

	// Закрытие ресурсов при выходе
	defer func() {
		close(stopChan)
		if err := tc.Disconnect(); err != nil {
			log.Errorf("Error disconnecting: %v", err)
		}
		tc.Close()
		if err := connect.Close(); err != nil {
			log.Errorf("Error closing ClickHouse connection: %v", err)
		}
		time.Sleep(time.Second)
	}()

	// Запуск мониторингов
	go maintainConnection()
	go monitorSystem()
	go monitorClickHouse()
	go reportMetrics()

	// Показываем доступные периоды
	log.Info("Available periods:")
	for _, kind := range tc.Data.CandleKinds.Items {
		log.Infof("Period: %d seconds (%d minutes)", kind.Period, kind.Period/60)
	}

	// Настройка экспорта сделок
	exportAllTradesSec := []string{}
	if envAllTrades := os.Getenv("EXPORT_ALL_TRADES"); envAllTrades != "" {
		for _, sec := range strings.Split(envAllTrades, ",") {
			if sec == "positions" {
				isAllTradesPositions = true
				continue
			}
			exportAllTradesSec = append(exportAllTradesSec, sec)
		}
	}

	go processTransaq()

	// Ждем подключения
	// Ждем подключения
	log.Info("Wait txmlconnector ")
	connectionTimeout := time.After(5 * time.Minute)
connectionLoop:
	for {
		select {
		case <-connectionTimeout:
			log.Fatal("Connection timeout exceeded")
		default:
			if tc.Data.ServerStatus.Connected == "true" {
				log.Info("Connected successfully")
				break connectionLoop // Используем метку для выхода из цикла
			}
			fmt.Print(".")
			time.Sleep(5 * time.Second)
		}
	}

	// Настройки для свечей
	if count, err := strconv.Atoi(os.Getenv("EXPORT_CANDLE_COUNT")); err == nil {
		dataCandleCount = count
	} else {
		dataCandleCount = 5000 // значение по умолчанию
	}

	// Проверяем режим реального времени
	if realtime := os.Getenv("REALTIME"); realtime == "1" {
		isRealtime = true
	}

	// Настройки для досок и инструментов
	exportSecBoards := []string{"TQBR", "TQCB", "FUT"}
	if eSecBoards := os.Getenv("EXPORT_SEC_BOARDS"); eSecBoards != "" {
		exportSecBoards = strings.Split(eSecBoards, ",")
	}

	exportSecCodes := []string{"ALL"}
	if eSecCodes := os.Getenv("EXPORT_SEC_CODES"); eSecCodes != "" {
		exportSecCodes = strings.Split(eSecCodes, ",")
	}

	if names := os.Getenv("EXPORT_SEC_INFO_NAMES"); names != "" {
		exportSecInfoNames = strings.Split(names, ",")
	}

	// Настройки периодов
	exportPeriodSeconds := []string{}
	if ePeriodSeconds := os.Getenv("EXPORT_PERIOD_SECONDS"); ePeriodSeconds != "" {
		exportPeriodSeconds = strings.Split(ePeriodSeconds, ",")
	}

	// Подготовка пакетной вставки
	batchSec, err := connect.PrepareBatch(ctx, ChSecuritiesInsertQuery)
	if err != nil {
		log.Fatal(err)
	}

	// Обработка инструментов
	for _, sec := range tc.Data.Securities.Items {
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Errorf("Recovered from panic processing security %s: %v\nStack trace:\n%s",
						sec.SecCode, r, debug.Stack())
				}
			}()

			// Проверяем доску
			exportSecBoardFound := false
			if slices.Contains(exportSecBoards, sec.Board) {
				exportSecBoardFound = true
			}

			// Проверяем нужно ли экспортировать сделки
			if exportSecBoardFound && slices.Contains(exportAllTradesSec, sec.SecCode) {
				allTrades.Items = append(allTrades.Items, sec.SecId)
			}

			// Проверяем облигации
			if sec.SecType == "BOND" {
				for _, secInfoName := range exportSecInfoNames {
					if strings.HasSuffix(sec.ShortName, secInfoName) {
						getSecuritiesInfo = append(getSecuritiesInfo, sec.SecId)
					}
				}
			}

			// Пропускаем неактивные инструменты
			if sec.SecId == 0 || sec.Active != "true" || len(sec.SecCode) > 16 {
				return
			}

			log.Debugf("Processing security: %+v", sec)

			// Добавляем информацию об инструменте
			if err := batchSec.Append(
				uint16(sec.SecId),
				sec.SecCode,
				sec.InstrClass,
				sec.Board,
				uint8(sec.Market),
				sec.ShortName,
				uint8(sec.Decimals),
				float32(sec.MinStep),
				uint8(sec.LotSize),
				float32(sec.PointCost),
				sec.SecType,
				uint8(sec.QuotesType),
				"", // добавляем пустую строку для поля sector
			); err != nil {
				log.Error(err)
			}

			// Пропускаем если не нужная доска
			if !exportSecBoardFound {
				return
			}

			// Пропускаем если нет кодов
			if len(exportSecCodes) == 0 {
				return
			}

			// Проверяем код инструмента
			exportSecCodeFound := false
			for _, exportSecCode := range exportSecCodes {
				if exportSecCode == sec.SecCode ||
					strings.Contains(sec.SecCode, exportSecCode) ||
					exportSecCode == sec.ShortName ||
					exportSecCode == "ALL" {
					exportSecCodeFound = true
					break
				}
			}

			if !exportSecCodeFound {
				return
			}

			// Добавляем в подписку
			quotations = append(quotations, commands.SubSecurity{SecId: sec.SecId})

			// Обработка периодов свечей
			for _, kind := range tc.Data.CandleKinds.Items {
				// Проверяем период если заданы конкретные периоды
				if len(exportPeriodSeconds) > 0 {
					periodFound := false
					for _, periodStr := range exportPeriodSeconds {
						period, err := strconv.Atoi(periodStr)
						if err != nil {
							log.Errorf("Invalid period format: %s", periodStr)
							continue
						}
						if period == kind.Period {
							periodFound = true
							break
						}
					}
					if !periodFound {
						continue
					}
				}

				log.Debugf("Processing candles: security=%s period=%d (%d min) name=%s",
					sec.SecCode, kind.Period, kind.Period/60, kind.Name)

				// Загружаем исторические данные
				if dataCandleCount > 0 {
					// Получаем последнюю свечу из БД
					var lastCandleTime time.Time
					query := fmt.Sprintf("SELECT MAX(date) FROM transaq_candles WHERE sec_code = '%s' AND period = %d", sec.SecCode, kind.Period)
					row := connect.QueryRow(ctx, query)
					if err := row.Scan(&lastCandleTime); err != nil && err != sql.ErrNoRows {
						log.Errorf("Error getting last candle: %v", err)
						continue
					}

					// Если свечей нет, загружаем все
					if lastCandleTime.IsZero() {
						cmd := commands.Command{
							Id:     "gethistorydata",
							Client: "FZTC29276A",
							Period: kind.ID,
							SecId:  sec.SecId,
							Count:  dataCandleCount,
							Reset:  "true",
						}
						log.Infof("Загрузка всех исторических данных: %s, период=%d", sec.SecCode, kind.Period)
						if err = tc.SendCommand(cmd); err != nil {
							log.Errorf("Error requesting historical data: %v", err)
							continue
						}
					} else {
						// Вычисляем сколько свечей пропущено
						now := time.Now()
						missedCandles := int(now.Sub(lastCandleTime).Seconds() / float64(kind.Period))

						if missedCandles > 0 {
							cmd := commands.Command{
								Id:     "gethistorydata",
								Client: "FZTC29276A",
								Period: kind.ID,
								SecId:  sec.SecId,
								Count:  missedCandles,
								Reset:  "false", // Не сбрасываем существующие данные
							}
							log.Infof("Загрузка пропущенных свечей (%d): %s, период=%d", missedCandles, sec.SecCode, kind.Period)
							if err = tc.SendCommand(cmd); err != nil {
								log.Errorf("Error requesting missed candles: %v", err)
								continue
							}
						}
					}
				}

			}
		}()
	}

	// Отправляем накопленные данные
	if batchSec.Rows() > 0 {
		if err := batchSec.Send(); err != nil {
			log.Error(err)
			metrics.ErrorCount++
		}
	}

	// Основной цикл работы
	<-tc.ShutdownChannel
}

func getPeriods() []int {
	periods := []int{300, 900, 3600, 7200, 14400, 86400} // периоды по умолчанию

	if customPeriods := os.Getenv("EXPORT_PERIOD_SECONDS"); customPeriods != "" {
		var parsedPeriods []int
		for _, p := range strings.Split(customPeriods, ",") {
			if period, err := strconv.Atoi(p); err == nil {
				parsedPeriods = append(parsedPeriods, period)
			}
		}
		if len(parsedPeriods) > 0 {
			periods = parsedPeriods
		}
	}
	return periods
}
