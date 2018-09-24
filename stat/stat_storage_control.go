package stat

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/KyberNetwork/reserve-data/common"
)

func (rs ReserveStats) ControllPriceAnalyticSize() error {
	tmpDir, err := ioutil.TempDir("", "ExpiredPriceAnalyticData")
	if err != nil {
		return err
	}

	defer func() {
		if rErr := os.RemoveAll(tmpDir); rErr != nil {
			log.Printf("failed to cleanup temp dir: %s, err : %s", tmpDir, rErr.Error())
		}
	}()

	for {
		log.Printf("StatPruner: waiting for signal from analytic storage control channel")
		t := <-rs.storageController.Runner.GetAnalyticStorageControlTicker()
		timepoint := common.TimeToTimepoint(t)
		log.Printf("StatPruner: got signal in analytic storage control channel with timestamp %d", timepoint)
		fileName := filepath.Join(tmpDir, fmt.Sprintf("ExpiredPriceAnalyticData_at_%s", time.Unix(int64(timepoint/1000), 0).UTC()))
		log.Printf("StatPruner: %s", fileName)
		nRecord, err := rs.analyticStorage.ExportExpiredPriceAnalyticData(common.GetTimepoint(), fileName)
		if err != nil {
			log.Printf("ERROR: StatPruner export Price Analytic operation failed: %s", err)
		} else {
			var integrity bool
			if nRecord > 0 {
				err = rs.storageController.Arch.UploadFile(rs.storageController.Arch.GetStatDataBucketName(), rs.storageController.ExpiredPriceAnalyticPath, fileName)
				if err != nil {
					log.Printf("StatPruner: Upload file failed: %s", err)
				} else {
					integrity, err = rs.storageController.Arch.CheckFileIntergrity(rs.storageController.Arch.GetStatDataBucketName(), rs.storageController.ExpiredPriceAnalyticPath, fileName)
					if err != nil {
						log.Printf("ERROR: StatPruner: error in file integrity check (%s):", err)
					}
					if !integrity {
						log.Printf("ERROR: StatPruner: file upload corrupted")

					}
					if err != nil || !integrity {
						//if the intergrity check failed, remove the remote file.
						removalErr := rs.storageController.Arch.RemoveFile(rs.storageController.Arch.GetStatDataBucketName(), rs.storageController.ExpiredPriceAnalyticPath, fileName)
						if removalErr != nil {
							log.Printf("ERROR: StatPruner: cannot remove remote file :(%s)", removalErr)
						}
					}
				}
			}
			if integrity && err == nil {
				nPrunedRecords, err := rs.analyticStorage.PruneExpiredPriceAnalyticData(common.TimeToTimepoint(t))
				if err != nil {
					log.Printf("StatPruner: cannot prune Price Analytic Data (%s)", err)
				} else if nPrunedRecords != nRecord {
					log.Printf("StatPruner: Number of exported Data is %d, which is different from number of Pruned Data %d", nRecord, nPrunedRecords)
				} else {
					log.Printf("StatPruner: exported and pruned %d expired records from Price Analytic Data", nRecord)
				}
			}
		}
		if err := os.Remove(fileName); err != nil {
			log.Fatal(err)
		}
	}
}

// uploadAndVerify upload the file to remote storage and check its integrity
// return error if occur and backup integrity

func (rs ReserveStats) uploadAndVerify(fileName, remotePath string) (bool, error) {
	err := rs.storageController.Arch.UploadFile(rs.storageController.Arch.GetStatDataBucketName(), remotePath, fileName)
	if err != nil {
		return false, err
	}

	integrity, err := rs.storageController.Arch.CheckFileIntergrity(rs.storageController.Arch.GetStatDataBucketName(), remotePath, fileName)
	if err != nil {
		log.Printf("ERROR: StatPruner: error in file integrity check (%s):", err)
		return false, err
	}

	//if the integrity check doesn't meet any error but the integrity is false, remove it from remote storage
	if !integrity {
		log.Printf("ERROR: StatPruner: file upload corrupted")
		removalErr := rs.storageController.Arch.RemoveFile(rs.storageController.Arch.GetStatDataBucketName(), remotePath, fileName)
		if removalErr != nil {
			log.Printf("ERROR: StatPruner: cannot remove remote file :(%s)", removalErr)
		}
		return integrity, removalErr
	}
	return true, nil
}

// ControlRateSize will check the rate database and export all the record that is more than 30 days old from now
// It will export these record to mutiple gz file, each contain all the expired record in a certain date
// in format ExpiredRateData_<firstTimestamp>_<lastTimestmap>.gz, in which time stamp is second
func (rs ReserveStats) ControlRateSize() error {
	tmpDir, err := ioutil.TempDir("", "ExpiredRateData")
	if err != nil {
		return err
	}

	defer func() {
		if rErr := os.RemoveAll(tmpDir); rErr != nil {
			log.Printf("failed to cleanup temp dir: %s, err : %s", tmpDir, rErr.Error())
		}
	}()

	for {
		//continuously pruning until there is no more expired data.
		for {
			tempfileName := filepath.Join(tmpDir, "TempExpireRateData.gz")
			fromTime, toTime, nRecord, err := rs.rateStorage.ExportExpiredRateData(common.GetTimepoint(), tempfileName)
			fileName := filepath.Join(tmpDir, fmt.Sprintf("ExpiredRateData_%d_%d.gz", fromTime/1000, toTime/1000))
			log.Printf("StatPruner: %s", fileName)
			if rErr := os.Rename(tempfileName, fileName); rErr != nil {
				log.Printf("StatPruner: cannot rename file (%s)", rErr)
				break
			}
			if err != nil {
				log.Printf("StatPruner ERROR: StatPruner export Price Analytic operation failed: %s", err)
				break
			} else {
				if nRecord > 0 {
					integrity, err := rs.uploadAndVerify(fileName, rs.storageController.ExpiredRatePath)
					if integrity && err == nil {
						nPrunedRecords, err := rs.rateStorage.PruneExpiredReserveRateData(toTime)
						if err != nil {
							log.Printf("StatPruner: cannot prune Reserve rate Data (%s)", err)
						} else if nPrunedRecords != nRecord {
							log.Printf("StatPruner: Number of exported Data is %d, which is different from number of Pruned Data %d", nRecord, nPrunedRecords)
						} else {
							log.Printf("StatPruner: exported and pruned %d expired records from Reserve rate Data", nRecord)
						}
					}
				} else {
					log.Printf("StatPruner: No expired record, exit and wait for next ticker")
					//if there is no expired record, break this loop
					break
				}

			}
			// remove the temp file
			if err := os.Remove(fileName); err != nil {
				log.Fatal(err)
			}
		}
		//Wait till next ticker
		t := <-rs.storageController.Runner.GetRateStorageControlTicker()
		timepoint := common.TimeToTimepoint(t)
		log.Printf("StatPruner: got signal in rate storage control channel with timestamp %d", timepoint)
	}
}

func (rs ReserveStats) RunStorageController() error {
	err := rs.storageController.Runner.Start()
	if err != nil {
		return err
	}
	go func() {
		if cErr := rs.ControllPriceAnalyticSize(); cErr != nil {
			log.Printf("Control price analytic failed: %s", cErr.Error())
		}
	}()
	go func() {
		if rErr := rs.ControlRateSize(); rErr != nil {
			log.Printf("StatPruner: Control rate analytic failed: %s", rErr.Error())
		}
	}()
	return err
}
