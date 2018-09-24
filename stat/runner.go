package stat

import (
	"time"
)

// FetcherRunner contains the tickers that control the fetcher services.
// Fetcher jobs will wait for control signal from tickers to start running its job.
type FetcherRunner interface {
	GetBlockTicker() <-chan time.Time
	GetLogTicker() <-chan time.Time
	GetReserveRatesTicker() <-chan time.Time
	GetTradeLogProcessorTicker() <-chan time.Time
	GetCatLogProcessorTicker() <-chan time.Time
	Start() error
	Stop() error
}

type TickerRunner struct {
	blockDuration       time.Duration
	logDuration         time.Duration
	rateDuration        time.Duration
	tlogProcessDuration time.Duration
	clogProcessDuration time.Duration
	blockClock          *time.Ticker
	logClock            *time.Ticker
	rateClock           *time.Ticker
	tlogProcessClock    *time.Ticker
	clogProcessClock    *time.Ticker
	signal              chan bool
}

func (t *TickerRunner) GetBlockTicker() <-chan time.Time {
	if t.blockClock == nil {
		<-t.signal
	}
	return t.blockClock.C
}

func (t *TickerRunner) GetLogTicker() <-chan time.Time {
	if t.logClock == nil {
		<-t.signal
	}
	return t.logClock.C
}

func (t *TickerRunner) GetReserveRatesTicker() <-chan time.Time {
	if t.rateClock == nil {
		<-t.signal
	}
	return t.rateClock.C
}

func (t *TickerRunner) GetTradeLogProcessorTicker() <-chan time.Time {
	if t.tlogProcessClock == nil {
		<-t.signal
	}
	return t.tlogProcessClock.C
}

func (t *TickerRunner) GetCatLogProcessorTicker() <-chan time.Time {
	if t.clogProcessClock == nil {
		<-t.signal
	}
	return t.clogProcessClock.C
}

func (t *TickerRunner) Start() error {
	t.blockClock = time.NewTicker(t.blockDuration)
	t.signal <- true
	t.logClock = time.NewTicker(t.logDuration)
	t.signal <- true
	t.rateClock = time.NewTicker(t.rateDuration)
	t.signal <- true
	t.tlogProcessClock = time.NewTicker(t.tlogProcessDuration)
	t.signal <- true
	t.clogProcessClock = time.NewTicker(t.clogProcessDuration)
	t.signal <- true
	return nil
}

func (t *TickerRunner) Stop() error {
	t.blockClock.Stop()
	t.logClock.Stop()
	t.rateClock.Stop()
	t.tlogProcessClock.Stop()
	t.clogProcessClock.Stop()
	return nil
}

func NewTickerRunner(
	blockDuration, logDuration, rateDuration, tlogProcessDuration, clogProcessDuration time.Duration) *TickerRunner {
	return &TickerRunner{
		blockDuration,
		logDuration,
		rateDuration,
		tlogProcessDuration,
		clogProcessDuration,
		nil,
		nil,
		nil,
		nil,
		nil,
		make(chan bool, 5),
	}
}
