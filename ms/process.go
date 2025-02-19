package main

import (
	"fmt"
	"math"
	"os"
	"sync/atomic"
	"time"

	"github.com/kmlebedev/txmlconnector/client/commands"
	log "github.com/sirupsen/logrus"
)

type CandleKey struct {
	SecId  int
	Period int
}

type QuoteAggregator struct {
	data       map[QuoteKey]*QuoteAggregation
	lastUpdate time.Time
	interval   time.Duration
}

type QuoteKey struct {
	SecId   int
	Board   string
	SecCode string
	Price   float64
	Side    string
}

type QuoteAggregation struct {
	MinVolume   int32
	MaxVolume   int32
	TotalVolume int64
	Updates     uint16
	EndVolume   int32
}

func NewQuoteAggregator(interval time.Duration) *QuoteAggregator {
	return &QuoteAggregator{
		data:     make(map[QuoteKey]*QuoteAggregation),
		interval: interval,
	}
}

func (qa *QuoteAggregator) AddQuote(quote *commands.Quote) {
	key := QuoteKey{
		SecId:   quote.SecId,
		Board:   quote.Board,
		SecCode: quote.SecCode,
		Price:   quote.Price,
	}

	// Отдельно для bid и ask
	if quote.Buy > 0 {
		key.Side = "bid"
		qa.addVolume(key, quote.Buy)
	}
	if quote.Sell > 0 {
		key.Side = "ask"
		qa.addVolume(key, quote.Sell)
	}
}

func (qa *QuoteAggregator) addVolume(key QuoteKey, volume int) {
	agg, exists := qa.data[key]
	if !exists {
		agg = &QuoteAggregation{
			MinVolume:   int32(volume),
			MaxVolume:   int32(volume),
			TotalVolume: int64(volume),
			Updates:     1,
			EndVolume:   int32(volume),
		}
		qa.data[key] = agg
	} else {
		agg.MinVolume = int32(math.Min(float64(agg.MinVolume), float64(volume)))
		agg.MaxVolume = int32(math.Max(float64(agg.MaxVolume), float64(volume)))
		agg.TotalVolume += int64(volume)
		agg.Updates++
		agg.EndVolume = int32(volume)
	}
}

func filterSecuritiesByBoard(securities []commands.Security, targetBoard string) []commands.Security {
	if targetBoard == "" || targetBoard == "ALL" {
		return securities
	}

	filtered := make([]commands.Security, 0)
	for _, sec := range securities {
		if sec.Board == targetBoard {
			filtered = append(filtered, sec)
		}
	}
	return filtered
}

var (
	quoteAggregator        = NewQuoteAggregator(30 * time.Second)
	version         uint32 = 1
)

func processTransaq() {

	var status commands.ServerStatus

	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case upd := <-tc.SecInfoUpdChan:
			log.Infof("secInfoUpd %+v", upd)

		case status = <-tc.ServerStatusChan:
			switch status.Connected {
			case "true":
				log.Infof("server status is true")

				quotations = []commands.SubSecurity{}
				allTrades.Items = []int{}

				// Получаем значение EXPORT_SEC_BOARDS из окружения
				targetBoard := os.Getenv("EXPORT_SEC_BOARDS")

				// Фильтруем инструменты по board
				filteredSecurities := filterSecuritiesByBoard(tc.Data.Securities.Items, targetBoard)

				log.Infof("Отфильтровано инструментов для board %s: %d из %d",
					targetBoard,
					len(filteredSecurities),
					len(tc.Data.Securities.Items))

				// Подписываемся только на отфильтрованные инструменты
				for _, sec := range filteredSecurities {
					log.Debugf("Подписка на: %s (board: %s)", sec.SecCode, sec.Board)
					quotations = append(quotations, commands.SubSecurity{
						SecId: sec.SecId,
					})
					allTrades.Items = append(allTrades.Items, sec.SecId)
				}

				log.Infof("Подписываемся на %d инструментов", len(quotations))

				cmd := commands.Command{
					Id:         "subscribe",
					Client:     "TCTEST",
					AllTrades:  allTrades,
					Quotations: quotations,
				}

				log.Infof("Отправляем команду: %+v", cmd)

				if err := tc.SendCommand(cmd); err != nil {
					log.Error("Ошибка подписки: ", err)
				}

			case "error":
				log.Warnf("txmlconnector not connected %+v\n", status)

			default:
				log.Infof("Status %+v", status)
			}

		case <-ticker.C:
			if status.Connected == "true" {
				continue
			}
			if err := tc.Connect(); err != nil {
				log.Error("reconnect", err)
			}

		case trades := <-tc.AllTradesChan:
			for _, trade := range trades.Items {
				select {
				case tradesToProcess <- &trade:
					// успешно отправили в обработку
				default:
					log.Error("Trade processing channel is full, dropping trade")
					atomic.AddInt64(&metrics.TradesErrorCount, 1)
				}
			}

		case quotes := <-tc.QuotesChan:
			now := time.Now()

			// Добавляем данные в агрегатор
			for _, quote := range quotes.Items {
				quoteAggregator.AddQuote(&quote)
			}

			// Проверяем нужно ли сохранять агрегацию
			if now.Sub(quoteAggregator.lastUpdate) >= quoteAggregator.interval {
				batch, err := connect.PrepareBatch(ctx, ChQuotesAggregatedInsert)
				if err != nil {
					log.Error(err)
					continue
				}

				// Собираем все данные в батч
				for key, agg := range quoteAggregator.data {
					avgVolume := float32(agg.TotalVolume) / float32(agg.Updates)

					side := int8(1) // bid
					if key.Side == "ask" {
						side = -1
					}

					if err := batch.Append(
						now,
						key.SecId,
						key.Board,
						key.SecCode,
						float32(key.Price),
						side,
						agg.MinVolume,
						agg.MaxVolume,
						avgVolume,
						agg.EndVolume,
						agg.Updates,
						version,
					); err != nil {
						log.Error(err)
						batch.Abort()
						continue
					}
				}

				// Отправляем батч
				if err := batch.Send(); err != nil {
					log.Error(err)
					continue
				}

				// Только после успешной отправки обновляем состояние
				quoteAggregator.data = make(map[QuoteKey]*QuoteAggregation)
				quoteAggregator.lastUpdate = now
				version++
			}

		case secInfo := <-tc.SecInfoChan:
			if err := insertSecInfo(&secInfo); err != nil {
				log.Errorf("trades async insert secInfo: %+v: %+v", secInfo, err)
			}

		case resp := <-tc.ResponseChannel:
			switch resp {
			case "united_portfolio":
				log.Infof(fmt.Sprintf("UnitedPortfolio: ```\n%+v\n```", tc.Data.UnitedPortfolio))

			case "united_equity":
				log.Infof(fmt.Sprintf("UnitedEquity: ```\n%+v\n```", tc.Data.UnitedEquity))

			case "positions":
				if tc.Data.Positions.UnitedLimits != nil && len(tc.Data.Positions.UnitedLimits) > 0 {
					positions.UnitedLimits = tc.Data.Positions.UnitedLimits
				}
				if tc.Data.Positions.SecPositions != nil && len(tc.Data.Positions.SecPositions) > 0 {
					positions.SecPositions = tc.Data.Positions.SecPositions
				}
				if tc.Data.Positions.FortsMoney != nil && len(tc.Data.Positions.FortsMoney) > 0 {
					positions.FortsMoney = tc.Data.Positions.FortsMoney
				}
				if tc.Data.Positions.MoneyPosition != nil && len(tc.Data.Positions.MoneyPosition) > 0 {
					positions.MoneyPosition = tc.Data.Positions.MoneyPosition
				}
				if tc.Data.Positions.FortsPosition != nil && len(tc.Data.Positions.FortsPosition) > 0 {
					positions.FortsPosition = tc.Data.Positions.FortsPosition
				}
				if tc.Data.Positions.FortsCollaterals != nil && len(tc.Data.Positions.FortsCollaterals) > 0 {
					positions.FortsCollaterals = tc.Data.Positions.FortsCollaterals
				}
				if tc.Data.Positions.SpotLimit != nil && len(tc.Data.Positions.SpotLimit) > 0 {
					positions.SpotLimit = tc.Data.Positions.SpotLimit
				}
				if isAllTradesPositions {
					for _, secPosition := range tc.Data.Positions.SecPositions {
						allTrades.Items = append(allTrades.Items, secPosition.SecId)
					}
				}
				log.Infof("Positions: \n%+v\n", tc.Data.Positions)

			case "candles":
				batch, err := connect.PrepareBatch(ctx, ChCandlesInsertQuery)
				if err != nil {
					log.Error(err)
					continue
				}

				moscowLoc, err := time.LoadLocation("Europe/Moscow")
				if err != nil {
					log.Errorf("Ошибка загрузки временной зоны: %v", err)
					batch.Abort()
					continue
				}

				// Сначала собираем все данные
				success := true
				for _, candle := range tc.Data.Candles.Items {
					candleDate, err := time.ParseInLocation("02.01.2006 15:04:05", candle.Date, moscowLoc)
					if err != nil {
						log.Errorf("Ошибка парсинга даты: %v", err)
						success = false
						break
					}

					if err := batch.Append(
						candleDate,
						tc.Data.Candles.SecCode,
						uint8(tc.Data.Candles.Period),
						float32(candle.Open),
						float32(candle.Close),
						float32(candle.High),
						float32(candle.Low),
						uint64(candle.Volume),
					); err != nil {
						log.Error(err)
						success = false
						break
					}
				}

				// Отправляем только если все данные успешно добавлены
				if success {
					if err := batch.Send(); err != nil {
						log.Error(err)
					}
				} else {
					batch.Abort()
				}

			case "quotations":
				// Проверяем статус торгов по первой котировке
				if len(tc.Data.Quotations.Items) > 0 {
					quotation := tc.Data.Quotations.Items[0]
					var statusMsg string
					switch quotation.TradingStatus {
					case "N":
						statusMsg = "Торги недоступны"
					case "C":
						statusMsg = "Торги закрыты"
					case "B":
						statusMsg = "Перерыв в торгах"
					case "T":
						statusMsg = "Идет торговая сессия"
					case "O":
						statusMsg = "Период открытия торгов"
					case "F":
						statusMsg = "Период закрытия торгов"
					case "L":
						statusMsg = "Период послеторгового аукциона"
					default:
						statusMsg = "Неизвестный статус торгов: " + quotation.TradingStatus
					}
					log.Info(statusMsg)

					switch quotation.Status {
					case "A":
						log.Info("Операции разрешены")
					case "N":
						log.Info("Заблокировано для торгов, разрешено исполнение сделок")
					case "S":
						log.Info("Операции запрещены")
					}
				}
			default:
				log.Debugf(fmt.Sprintf("receive %s", resp))
			}
		}
	}
}
