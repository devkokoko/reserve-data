package settings

import (
	"errors"
	"fmt"
	"log"

	"github.com/KyberNetwork/reserve-data/common"
	ethereum "github.com/ethereum/go-ethereum/common"
)

// ErrTokenNotFound is the error returned for get operation where the
// token is not found in database.
var ErrTokenNotFound = errors.New("token not found")

func (setting *Settings) GetAllTokens() ([]common.Token, error) {
	return setting.Tokens.Storage.GetAllTokens()
}

func (setting *Settings) GetActiveTokens() ([]common.Token, error) {
	return setting.Tokens.Storage.GetActiveTokens()
}

func (setting *Settings) GetInternalTokens() ([]common.Token, error) {
	return setting.Tokens.Storage.GetInternalTokens()
}

func (setting *Settings) GetExternalTokens() ([]common.Token, error) {
	return setting.Tokens.Storage.GetExternalTokens()
}

func (setting *Settings) GetTokenByID(id string) (common.Token, error) {
	return setting.Tokens.Storage.GetTokenByID(id)
}

func (setting *Settings) GetActiveTokenByID(id string) (common.Token, error) {
	return setting.Tokens.Storage.GetActiveTokenByID(id)
}

func (setting *Settings) GetInternalTokenByID(id string) (common.Token, error) {
	return setting.Tokens.Storage.GetInternalTokenByID(id)
}

func (setting *Settings) GetExternalTokenByID(id string) (common.Token, error) {
	return setting.Tokens.Storage.GetExternalTokenByID(id)
}

func (setting *Settings) GetTokenByAddress(addr ethereum.Address) (common.Token, error) {
	return setting.Tokens.Storage.GetTokenByAddress(addr)
}

func (setting *Settings) GetActiveTokenByAddress(addr ethereum.Address) (common.Token, error) {
	return setting.Tokens.Storage.GetActiveTokenByAddress(addr)
}

func (setting *Settings) GetInternalTokenByAddress(addr ethereum.Address) (common.Token, error) {
	return setting.Tokens.Storage.GetInternalTokenByAddress(addr)
}

func (setting *Settings) GetExternalTokenByAddress(addr ethereum.Address) (common.Token, error) {
	return setting.Tokens.Storage.GetExternalTokenByAddress(addr)
}

func (setting *Settings) ETHToken() common.Token {
	eth, err := setting.Tokens.Storage.GetInternalTokenByID("ETH")
	if err != nil {
		log.Panicf("There is no ETH token in token DB, this should not happen (%s)", err)
	}
	return eth
}

func (setting *Settings) NewTokenPairFromID(base, quote string) (common.TokenPair, error) {
	bToken, err1 := setting.GetInternalTokenByID(base)
	qToken, err2 := setting.GetInternalTokenByID(quote)
	if err1 != nil || err2 != nil {
		return common.TokenPair{}, errors.New(fmt.Sprintf("%s or %s is not supported", base, quote))
	} else {
		return common.TokenPair{Base: bToken, Quote: qToken}, nil
	}
}

func (setting *Settings) MustCreateTokenPair(base, quote string) common.TokenPair {
	pair, err := setting.NewTokenPairFromID(base, quote)
	if err != nil {
		panic(err)
	}
	return pair
}

func (setting *Settings) UpdateToken(t common.Token, timestamp uint64) error {
	if timestamp == 0 {
		timestamp = common.GetTimepoint()
	}
	return setting.Tokens.Storage.UpdateToken(t, timestamp)
}

func (setting *Settings) ApplyTokenWithExchangeSetting(tokens []common.Token, exSetting map[ExchangeName]*common.ExchangeSetting, timestamp uint64) error {
	if timestamp == 0 {
		timestamp = common.GetTimepoint()
	}
	return setting.Tokens.Storage.UpdateTokenWithExchangeSetting(tokens, exSetting, timestamp)
}

func (setting *Settings) UpdatePendingTokenUpdates(tokenUpdates map[string]common.TokenUpdate) error {
	return setting.Tokens.Storage.StorePendingTokenUpdates(tokenUpdates)
}

func (setting *Settings) GetPendingTokenUpdates() (map[string]common.TokenUpdate, error) {
	return setting.Tokens.Storage.GetPendingTokenUpdates()
}

func (setting *Settings) RemovePendingTokenUpdates() error {
	return setting.Tokens.Storage.RemovePendingTokenUpdates()
}

func (setting *Settings) GetTokenVersion() (uint64, error) {
	return setting.Tokens.Storage.GetTokenVersion()
}
