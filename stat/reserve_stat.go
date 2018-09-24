package stat

import (
	"errors"
	"fmt"
	"log"
	"math/big"
	"sort"
	"strings"
	"time"

	"github.com/KyberNetwork/reserve-data/common"
	"github.com/KyberNetwork/reserve-data/common/archive"
	"github.com/KyberNetwork/reserve-data/common/blockchain"
	"github.com/KyberNetwork/reserve-data/stat/statpruner"
	ethereum "github.com/ethereum/go-ethereum/common"
)

const (
	maxGetRatesPeriod uint64 = 86400000 //1 days in milisec
)

type ReserveStats struct {
	analyticStorage   AnalyticStorage
	statStorage       StatStorage
	logStorage        LogStorage
	userStorage       UserStorage
	rateStorage       RateStorage
	feeSetRateStorage FeeSetRateStorage
	fetcher           *Fetcher
	storageController statpruner.StorageController
	cmcEthUSDRate     *blockchain.CMCEthUSDRate
	setting           Setting
}

func NewReserveStats(
	analyticStorage AnalyticStorage,
	statStorage StatStorage,
	logStorage LogStorage,
	rateStorage RateStorage,
	userStorage UserStorage,
	feeSetRateStorage FeeSetRateStorage,
	controllerRunner statpruner.ControllerRunner,
	fetcher *Fetcher,
	arch archive.Archive,
	cmcEthUSDRate *blockchain.CMCEthUSDRate,
	setting Setting) *ReserveStats {
	storageController, err := statpruner.NewStorageController(controllerRunner, arch)
	if err != nil {
		panic(err)
	}
	return &ReserveStats{
		analyticStorage:   analyticStorage,
		statStorage:       statStorage,
		logStorage:        logStorage,
		rateStorage:       rateStorage,
		userStorage:       userStorage,
		feeSetRateStorage: feeSetRateStorage,
		fetcher:           fetcher,
		storageController: storageController,
		cmcEthUSDRate:     cmcEthUSDRate,
		setting:           setting,
	}
}

func validateTimeWindow(fromTime, toTime uint64, freq string) (uint64, uint64, error) {
	var from = fromTime * 1000000
	var to = toTime * 1000000
	switch freq {
	case "m", "M":
		if to-from > uint64((time.Hour * 24).Nanoseconds()) {
			return 0, 0, errors.New("Minute frequency limit is 1 day")
		}
	case "h", "H":
		if to-from > uint64((time.Hour * 24 * 180).Nanoseconds()) {
			return 0, 0, errors.New("Hour frequency limit is 180 days")
		}
	case "d", "D":
		if to-from > uint64((time.Hour * 24 * 365 * 3).Nanoseconds()) {
			return 0, 0, errors.New("Day frequency limit is 3 years")
		}
	default:
		return 0, 0, errors.New("Invalid frequencies")
	}
	return from, to, nil
}

func (rs ReserveStats) GetAssetVolume(fromTime, toTime uint64, freq, asset string) (common.StatTicks, error) {
	data := common.StatTicks{}

	fromTime, toTime, err := validateTimeWindow(fromTime, toTime, freq)
	if err != nil {
		return data, err
	}

	token, err := rs.setting.GetActiveTokenByID(asset)
	if err != nil {
		return data, fmt.Errorf("assets %s is not supported", asset)
	}

	data, err = rs.statStorage.GetAssetVolume(fromTime, toTime, freq, ethereum.HexToAddress(token.Address))
	return data, err
}

func (rs ReserveStats) GetBurnFee(fromTime, toTime uint64, freq, reserveAddr string) (common.StatTicks, error) {
	data := common.StatTicks{}

	fromTime, toTime, err := validateTimeWindow(fromTime, toTime, freq)
	if err != nil {
		return data, err
	}

	data, err = rs.statStorage.GetBurnFee(fromTime, toTime, freq, ethereum.HexToAddress(reserveAddr))

	return data, err
}

func (rs ReserveStats) GetWalletFee(fromTime, toTime uint64, freq, reserveAddr, walletAddr string) (common.StatTicks, error) {
	data := common.StatTicks{}

	fromTime, toTime, err := validateTimeWindow(fromTime, toTime, freq)
	if err != nil {
		return data, err
	}

	data, err = rs.statStorage.GetWalletFee(fromTime, toTime, freq, ethereum.HexToAddress(reserveAddr), ethereum.HexToAddress(walletAddr))

	return data, err
}

func (rs ReserveStats) GetUserVolume(fromTime, toTime uint64, freq, userAddr string) (common.StatTicks, error) {
	data := common.StatTicks{}

	fromTime, toTime, err := validateTimeWindow(fromTime, toTime, freq)
	if err != nil {
		return data, err
	}

	data, err = rs.statStorage.GetUserVolume(fromTime, toTime, freq, ethereum.HexToAddress(userAddr))

	return data, err
}

func (rs ReserveStats) GetUsersVolume(fromTime, toTime uint64, freq string, userAddrs []string) (common.UsersVolume, error) {
	data := common.StatTicks{}
	result := common.UsersVolume{}

	fromTime, toTime, err := validateTimeWindow(fromTime, toTime, freq)
	if err != nil {
		return result, err
	}
	for _, userAddr := range userAddrs {
		data, err = rs.statStorage.GetUserVolume(fromTime, toTime, freq, ethereum.HexToAddress(userAddr))
		result[userAddr] = data
	}

	return result, err
}

func (rs ReserveStats) GetReserveVolume(fromTime, toTime uint64, freq, reserveAddr, tokenID string) (common.StatTicks, error) {
	data := common.StatTicks{}
	token, err := rs.setting.GetActiveTokenByID(tokenID)
	if err != nil {
		return data, err
	}
	fromTime, toTime, err = validateTimeWindow(fromTime, toTime, freq)
	if err != nil {
		return data, err
	}

	reserveAddr = strings.ToLower(reserveAddr)
	tokenAddr := strings.ToLower(token.Address)
	data, err = rs.statStorage.GetReserveVolume(fromTime, toTime, freq, ethereum.HexToAddress(reserveAddr), ethereum.HexToAddress(tokenAddr))
	return data, err
}

func (rs ReserveStats) GetTradeSummary(fromTime, toTime uint64, timezone int64) (common.StatTicks, error) {
	data := common.StatTicks{}

	fromTime, toTime, err := validateTimeWindow(fromTime, toTime, "D")
	if err != nil {
		return data, err
	}

	data, err = rs.statStorage.GetTradeSummary(fromTime, toTime, timezone)
	return data, err
}

func (rs ReserveStats) GetTradeLogs(fromTime uint64, toTime uint64) ([]common.TradeLog, error) {
	result := []common.TradeLog{}

	if toTime-fromTime > maxGetRatesPeriod {
		return result, fmt.Errorf("Time range is too broad, it must be smaller or equal to %d miliseconds", maxGetRatesPeriod)
	}

	result, err := rs.logStorage.GetTradeLogs(fromTime*1000000, toTime*1000000)
	return result, err
}

func (rs ReserveStats) GetGeoData(fromTime, toTime uint64, country string, tzparam int64) (common.StatTicks, error) {
	var err error
	result := common.StatTicks{}
	fromTime, toTime, err = validateTimeWindow(fromTime, toTime, "D")
	if err != nil {
		return result, err
	}
	result, err = rs.statStorage.GetCountryStats(fromTime, toTime, country, tzparam)
	return result, err
}

func (rs ReserveStats) GetHeatMap(fromTime, toTime uint64, tzparam int64) (common.HeatmapResponse, error) {
	result := common.Heatmap{}
	var arrResult common.HeatmapResponse
	var err error
	fromTime, toTime, err = validateTimeWindow(fromTime, toTime, "D")
	if err != nil {
		return arrResult, err
	}
	countries, err := rs.statStorage.GetCountries()
	if err != nil {
		return arrResult, err
	}

	// get stats
	for _, c := range countries {
		var cStats common.StatTicks
		if cStats, err = rs.statStorage.GetCountryStats(fromTime, toTime, c, tzparam); err != nil {
			return arrResult, err
		}
		for _, stat := range cStats {
			s, ok := stat.(common.MetricStats)
			if !ok {
				return arrResult, fmt.Errorf("cannot convert stat (%v) to MetricStat", s)
			}
			current := result[c]
			result[c] = common.HeatmapType{
				TotalETHValue:        current.TotalETHValue + s.ETHVolume,
				TotalFiatValue:       current.TotalFiatValue + s.USDVolume,
				ToTalBurnFee:         current.ToTalBurnFee + s.BurnFee,
				TotalTrade:           current.TotalTrade + s.TradeCount,
				TotalUniqueAddresses: current.TotalUniqueAddresses + s.UniqueAddr,
				TotalKYCUser:         current.TotalKYCUser + s.KYCEd,
			}
		}
	}

	// sort heatmap
	for k, v := range result {
		arrResult = append(arrResult, common.HeatmapObject{
			Country:              k,
			TotalETHValue:        v.TotalETHValue,
			TotalFiatValue:       v.TotalFiatValue,
			ToTalBurnFee:         v.ToTalBurnFee,
			TotalTrade:           v.TotalTrade,
			TotalUniqueAddresses: v.TotalUniqueAddresses,
			TotalKYCUser:         v.TotalKYCUser,
		})
	}
	sort.Sort(sort.Reverse(arrResult))
	return arrResult, err
}

func (rs ReserveStats) GetTokenHeatmap(fromTime, toTime uint64, tokenStr, freq string) (common.TokenHeatmapResponse, error) {
	result := common.CountryTokenHeatmap{}
	var arrResult common.TokenHeatmapResponse
	fromTime, toTime, err := validateTimeWindow(fromTime, toTime, "D")
	if err != nil {
		return arrResult, err
	}
	countries, err := rs.statStorage.GetCountries()
	if err != nil {
		return arrResult, err
	}
	token, err := rs.setting.GetActiveTokenByID(tokenStr)
	if err != nil {
		return arrResult, err
	}
	for _, country := range countries {
		var stats common.StatTicks
		key := fmt.Sprintf("%s_%s", country, strings.ToLower(token.Address))
		if stats, err = rs.statStorage.GetTokenHeatmap(fromTime, toTime, key, freq); err != nil {
			return arrResult, err
		}
		for _, stat := range stats {
			s, ok := stat.(common.VolumeStats)
			if !ok {
				return arrResult, fmt.Errorf("cannot convert stat (%v) to VolumeStats", s)
			}
			current := result[country]
			result[country] = common.VolumeStats{
				Volume:    current.Volume + s.Volume,
				ETHVolume: current.ETHVolume + s.ETHVolume,
				USDAmount: current.USDAmount + s.USDAmount,
			}
		}
	}
	for k, v := range result {
		arrResult = append(arrResult, common.TokenHeatmap{
			Country:   k,
			Volume:    v.Volume,
			ETHVolume: v.ETHVolume,
			USDVolume: v.USDAmount,
		})
	}
	sort.Sort(sort.Reverse(arrResult))
	return arrResult, err
}

func (rs ReserveStats) GetCountries() ([]string, error) {
	result, _ := rs.statStorage.GetCountries()
	return result, nil
}

func (rs ReserveStats) GetCatLogs(fromTime uint64, toTime uint64) ([]common.SetCatLog, error) {
	return rs.logStorage.GetCatLogs(fromTime, toTime)
}

func (rs ReserveStats) GetPendingAddresses() ([]string, error) {
	addresses, err := rs.userStorage.GetPendingAddresses()
	if err != nil {
		return nil, err
	}
	result := []string{}
	for _, addr := range addresses {
		result = append(result, common.AddrToString(addr))
	}
	return result, nil
}

func (rs ReserveStats) Run() error {
	return rs.fetcher.Run()
}

func (rs ReserveStats) Stop() error {
	return rs.fetcher.Stop()
}

func (rs ReserveStats) GetCapByAddress(addr ethereum.Address) (*common.UserCap, error) {
	category, err := rs.userStorage.GetCategory(addr)
	if err != nil {
		return nil, err
	}
	if category == "0x4" {
		return common.KycedCap(), nil
	}
	return common.NonKycedCap(), nil
}

//GetTxCapByAddress return user Tx limit by wei
//return true if address kyced, and return false if address is non-kyced
func (rs ReserveStats) GetTxCapByAddress(addr ethereum.Address) (*big.Int, bool, error) {
	email, err := rs.userStorage.GetKYCAddress(addr)
	if err != nil {
		return nil, false, err
	}
	var usdCap float64
	kyced := false
	if email != "" {
		usdCap = common.KycedCap().DailyLimit
		kyced = true
	} else {
		usdCap = common.NonKycedCap().TxLimit
	}
	timepoint := common.GetTimepoint()
	rate := rs.cmcEthUSDRate.GetUSDRate(timepoint)
	var txLimit *big.Int
	if rate == 0 {
		return txLimit, kyced, errors.New("cannot get eth usd rate from cmc")
	}
	ethLimit := usdCap / rate
	txLimit = common.EthToWei(ethLimit)
	return txLimit, kyced, nil
}

func (rs ReserveStats) GetCapByUser(userID string) (*common.UserCap, error) {
	addresses, _, err := rs.userStorage.GetAddressesOfUser(userID)
	if err != nil {
		return nil, err
	}
	if len(addresses) == 0 {
		log.Printf("Couldn't find any associated addresses. User %s is not kyced.", userID)
		return common.NonKycedCap(), nil
	}
	return rs.GetCapByAddress(addresses[0])
}

func isDuplicate(currentRate, latestRate common.ReserveRates) bool {
	currentData := currentRate.Data
	latestData := latestRate.Data
	for key := range currentData {
		if currentData[key].BuyReserveRate != latestData[key].BuyReserveRate ||
			currentData[key].BuySanityRate != latestData[key].BuySanityRate ||
			currentData[key].SellReserveRate != latestData[key].SellReserveRate ||
			currentData[key].SellSanityRate != latestData[key].SellSanityRate {
			return false
		}
	}
	return true
}
func (rs ReserveStats) GetWalletStats(fromTime uint64, toTime uint64, walletAddr string, timezone int64) (common.StatTicks, error) {
	fromTime, toTime, err := validateTimeWindow(fromTime, toTime, "D")
	if err != nil {
		return nil, err
	}
	walletAddr = strings.ToLower(walletAddr)
	return rs.statStorage.GetWalletStats(fromTime, toTime, ethereum.HexToAddress(walletAddr), timezone)
}

func (rs ReserveStats) GetWalletAddresses() ([]string, error) {
	return rs.statStorage.GetWalletAddresses()
}

func (rs ReserveStats) GetReserveRates(fromTime, toTime uint64, reserveAddr ethereum.Address) ([]common.ReserveRates, error) {
	var result []common.ReserveRates
	var err error
	var rates []common.ReserveRates
	rates, err = rs.rateStorage.GetReserveRates(fromTime, toTime, reserveAddr)
	latest := common.ReserveRates{}
	for _, rate := range rates {
		if !isDuplicate(rate, latest) {
			result = append(result, rate)
		} else {
			if len(result) > 0 {
				result[len(result)-1].ToBlockNumber = rate.BlockNumber
			}
		}
		latest = rate
	}
	log.Printf("Get reserve rate: %v", result)
	return result, err
}

func (rs ReserveStats) GetUserList(fromTime, toTime uint64, timezone int64) (common.UserListResponse, error) {
	fromTime, toTime, err := validateTimeWindow(fromTime, toTime, "D")
	if err != nil {
		return []common.UserInfo{}, err
	}
	result := common.UserListResponse{}
	data, err := rs.statStorage.GetUserList(fromTime, toTime, timezone)
	for _, v := range data {
		result = append(result, v)
	}
	sort.Sort(sort.Reverse(result))
	return result, err
}

func (rs ReserveStats) UpdateUserAddresses(userID string, addrs []ethereum.Address, timestamps []uint64) error {
	addresses := []ethereum.Address{}
	for _, addr := range addrs {
		addresses = append(addresses, addr)
	}
	return rs.userStorage.UpdateUserAddresses(userID, addresses, timestamps)
}

func (rs ReserveStats) ExceedDailyLimit(address ethereum.Address) (bool, error) {
	user, _, err := rs.userStorage.GetUserOfAddress(address)
	log.Printf("got user %s for address %s", user, strings.ToLower(address.Hex()))
	if err != nil {
		return false, err
	}
	addrs := []string{}
	if user == "" {
		// address is not associated to any users
		addrs = append(addrs, strings.ToLower(address.Hex()))
	} else {
		var addrs []ethereum.Address
		addrs, _, err = rs.userStorage.GetAddressesOfUser(user)
		log.Printf("got addresses %v for address %s", addrs, strings.ToLower(address.Hex()))
		if err != nil {
			return false, err
		}
	}
	today := common.GetTimepoint() / uint64(24*time.Hour/time.Millisecond) * uint64(24*time.Hour/time.Millisecond)
	var totalVolume float64
	for _, addr := range addrs {
		var volumeStats common.StatTicks
		volumeStats, err = rs.GetUserVolume(today-1, today, "D", addr)
		if err == nil {
			log.Printf("volumes: %+v", volumeStats)
			if len(volumeStats) == 0 {
			} else if len(volumeStats) > 1 {
				log.Printf("Got more than 1 day stats. This is a bug in GetUserVolume")
			} else {
				for _, volume := range volumeStats {
					volumeValue, ok := volume.(common.VolumeStats)
					if !ok {
						log.Printf("cannot convert volume (%v) to VolumeStats", volume)
						continue
					}
					totalVolume += volumeValue.USDAmount
					break
				}
			}
		} else {
			log.Printf("Getting volumes for %s failed, err: %s", strings.ToLower(address.Hex()), err.Error())
		}
	}
	cap, err := rs.GetCapByAddress(address)
	if err == nil && totalVolume >= cap.DailyLimit {
		return true, nil
	} else {
		return false, nil
	}
}

func (rs ReserveStats) UpdatePriceAnalyticData(timestamp uint64, value []byte) error {
	return rs.analyticStorage.UpdatePriceAnalyticData(timestamp, value)
}

func (rs ReserveStats) GetPriceAnalyticData(fromTime uint64, toTime uint64) ([]common.AnalyticPriceResponse, error) {
	return rs.analyticStorage.GetPriceAnalyticData(fromTime, toTime)
}

func (rs ReserveStats) GetFeeSetRateByDay(fromTime uint64, toTime uint64) ([]common.FeeSetRate, error) {
	return rs.feeSetRateStorage.GetFeeSetRateByDay(fromTime, toTime)
}
