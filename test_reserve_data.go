package reserve

import (
	"github.com/KyberNetwork/reserve-data/common"
)

type TestReserveData struct {
}

func (tsd TestReserveData) CurrentPriceVersion() common.Version {
	return common.Version(0)
}

func (tsd TestReserveData) GetAllPrices() (common.AllPriceResponse, error) {
	return common.AllPriceResponse{}, nil
}

func (tsd TestReserveData) GetOnePrice(common.TokenPairID) (common.OnePriceResponse, error) {
	return common.OnePriceResponse{}, nil
}

func (tsd TestReserveData) Run() error {
	return nil
}

func NewTestReserveData() *TestReserveData {
	return &TestReserveData{}
}
