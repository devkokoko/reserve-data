package storage

import (
	"errors"
	"fmt"
	"strings"

	"github.com/KyberNetwork/reserve-data/boltutil"
	"github.com/KyberNetwork/reserve-data/settings"
	"github.com/boltdb/bolt"
)

const address_version = "address_version"

func (boltSettingStorage *BoltSettingStorage) GetAllAddresses() (map[string]interface{}, error) {
	result := make(map[string]interface{})
	err := boltSettingStorage.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(ADDRESS_SETTING_BUCKET))
		if b == nil {
			return fmt.Errorf("Bucket %s has not existed yet", ADDRESS_SETTING_BUCKET)
		}
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			keyName := settings.AddressName(boltutil.BytesToUint64(k)).String()
			result[keyName] = string(v)
		}
		b = tx.Bucket([]byte(ADDRESS_SET_SETTING_BUCKET))
		if b == nil {
			return fmt.Errorf("Bucket %s has not existed yet", ADDRESS_SET_SETTING_BUCKET)
		}
		c = b.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			keyName := settings.AddressSetName(boltutil.BytesToUint64(k)).String()
			nestedBuck := b.Bucket(k)
			if nestedBuck == nil {
				return fmt.Errorf("Nested bucket error: key %s is not a bucket of addresses", keyName)
			}
			var adrset []string
			nC := nestedBuck.Cursor()
			for nK, _ := nC.First(); nK != nil; nK, _ = nC.Next() {
				adrset = append(adrset, string(nK))
			}
			result[keyName] = adrset
		}
		return nil
	})
	return result, err
}

func updateAddressVersion(tx *bolt.Tx, timestamp uint64) error {
	b := tx.Bucket([]byte(address_version))
	return b.Put([]byte(address_version), boltutil.Uint64ToBytes(timestamp))
}

func (boltSettingStorage *BoltSettingStorage) GetAddressVersion() (uint64, error) {
	var result uint64
	err := boltSettingStorage.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(address_version))
		v := b.Get([]byte(address_version))
		if v == nil {
			return errors.New("Cannot find address version")
		}
		result = boltutil.BytesToUint64(v)
		return nil
	})
	return result, err
}

func (boltSettingStorage *BoltSettingStorage) UpdateOneAddress(name settings.AddressName, address string, timestamp uint64) error {
	address = strings.ToLower(address)
	err := boltSettingStorage.db.Update(func(tx *bolt.Tx) error {
		if uErr := updateAddressVersion(tx, timestamp); uErr != nil {
			return uErr
		}
		b, uErr := tx.CreateBucketIfNotExists([]byte(ADDRESS_SETTING_BUCKET))
		if uErr != nil {
			return uErr
		}
		return b.Put(boltutil.Uint64ToBytes(uint64(name)), []byte(address))
	})
	return err
}

func (boltSettingStorage *BoltSettingStorage) GetAddress(add settings.AddressName) (string, error) {
	var address string
	err := boltSettingStorage.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(ADDRESS_SETTING_BUCKET))
		if b == nil {
			return fmt.Errorf("Bucket doesn't exist yet")
		}
		data := b.Get(boltutil.Uint64ToBytes(uint64(add)))
		if data == nil {
			return fmt.Errorf("Key %s is not found", add)
		}
		address = string(data)
		return nil
	})
	return address, err
}

func (boltSettingStorage *BoltSettingStorage) AddAddressToSet(setName settings.AddressSetName, address string, timestamp uint64) error {
	address = strings.ToLower(address)
	defaultValue := "1"
	err := boltSettingStorage.db.Update(func(tx *bolt.Tx) error {
		if uErr := updateAddressVersion(tx, timestamp); uErr != nil {
			return uErr
		}
		b, uErr := tx.CreateBucketIfNotExists([]byte(ADDRESS_SET_SETTING_BUCKET))
		if uErr != nil {
			return uErr
		}
		s, uErr := b.CreateBucketIfNotExists(boltutil.Uint64ToBytes(uint64(setName)))
		if uErr != nil {
			return uErr
		}
		return s.Put([]byte(address), []byte(defaultValue))

	})
	return err
}

func (boltSettingStorage *BoltSettingStorage) GetAddresses(setName settings.AddressSetName) ([]string, error) {
	result := []string{}
	err := boltSettingStorage.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(ADDRESS_SET_SETTING_BUCKET))
		if b == nil {
			return errors.New("Bucket doesn't exist yet")
		}
		s := b.Bucket(boltutil.Uint64ToBytes(uint64(setName)))
		if s == nil {
			return fmt.Errorf("Address set with name %s doesn't exist", setName)
		}
		c := s.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			result = append(result, string(k))
		}
		return nil
	})
	return result, err
}

func CountAddressSetBucket(b *bolt.Bucket) (uint64, error) {
	var result uint64
	c := b.Cursor()
	for setName, _ := c.First(); setName != nil; setName, _ = c.Next() {
		s := b.Bucket(setName)
		if s == nil {
			return 0, fmt.Errorf("bucket %s is not available", string(setName))
		}
		sc := s.Cursor()
		for addr, _ := sc.First(); addr != nil; addr, _ = sc.Next() {
			result += 1
		}
	}
	return result, nil
}

func CountAddressBucket(b *bolt.Bucket) uint64 {
	var result uint64
	c := b.Cursor()
	for name, _ := c.First(); name != nil; name, _ = c.Next() {
		result += 1
	}
	return result
}

func (boltSettingStorage *BoltSettingStorage) CountAddress() (uint64, error) {
	var result uint64
	err := boltSettingStorage.db.View(func(tx *bolt.Tx) error {

		b := tx.Bucket([]byte(ADDRESS_SET_SETTING_BUCKET))
		if b == nil {
			return fmt.Errorf("bucket %s hasn't existed yet", ADDRESS_SET_SETTING_BUCKET)
		}
		count, uErr := CountAddressSetBucket(b)
		if uErr != nil {
			return uErr
		}
		result += count
		b = tx.Bucket([]byte(ADDRESS_SETTING_BUCKET))
		if b == nil {
			return fmt.Errorf("bucket %s hasn't existed yet", ADDRESS_SETTING_BUCKET)
		}
		result += CountAddressBucket(b)
		return nil
	})
	if err != nil {
		return 0, err
	}
	return result, nil
}