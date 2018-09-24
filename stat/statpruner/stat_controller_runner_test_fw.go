package statpruner

import "log"

type ControllerRunnerTest struct {
	cr ControllerRunner
}

func NewControllerRunnerTest(controllerRunner ControllerRunner) *ControllerRunnerTest {
	return &ControllerRunnerTest{controllerRunner}
}

func (crt *ControllerRunnerTest) TestAnalyticStorageControlTicker(nanosec int64) error {
	if err := crt.cr.Start(); err != nil {
		return err
	}
	t := <-crt.cr.GetAnalyticStorageControlTicker()
	log.Printf("ticked: %s", t.String())
	if err := crt.cr.Stop(); err != nil {
		return err
	}
	return nil
}
