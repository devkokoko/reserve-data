package statpruner

import (
	"time"
)

// ControllerRunner contains the tickers that control the pruner services.
type ControllerRunner interface {
	GetAnalyticStorageControlTicker() <-chan time.Time
	GetRateStorageControlTicker() <-chan time.Time
	Start() error
	Stop() error
}

type ControllerTickerRunner struct {
	ascDuration  time.Duration
	rateDuration time.Duration
	ascClock     *time.Ticker
	rateClock    *time.Ticker
	signal       chan bool
}

func (ctr *ControllerTickerRunner) GetAnalyticStorageControlTicker() <-chan time.Time {
	if ctr.ascClock == nil {
		<-ctr.signal
	}
	return ctr.ascClock.C
}

func (ctr *ControllerTickerRunner) GetRateStorageControlTicker() <-chan time.Time {
	if ctr.rateClock == nil {
		<-ctr.signal
	}
	return ctr.rateClock.C
}

func (ctr *ControllerTickerRunner) Start() error {
	ctr.ascClock = time.NewTicker(ctr.ascDuration)
	ctr.signal <- true
	ctr.rateClock = time.NewTicker(ctr.rateDuration)
	ctr.signal <- true
	return nil
}

func (ctr *ControllerTickerRunner) Stop() error {
	ctr.rateClock.Stop()
	ctr.ascClock.Stop()
	return nil
}

func NewControllerTickerRunner(
	ascDuration time.Duration,
	rateDuration time.Duration) *ControllerTickerRunner {
	return &ControllerTickerRunner{
		ascDuration:  ascDuration,
		rateDuration: rateDuration,
		ascClock:     nil,
		rateClock:    nil,
		signal:       make(chan bool, 2),
	}
}
