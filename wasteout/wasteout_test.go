//  wasteout.go
// Get Data from  wasteout.ru
package wasteout

import (
  "testing"
  "github.com/stretchr/testify/assert"
  "github.com/golang/glog"
  "flag"
  "fmt"
  "time"

  "github.com/Lunkov/lib-mc"
)

func TestCheckWasteOut(t *testing.T) {
  fmt.Println("TestFlag testing:")

  flag.Set("alsologtostderr", "true")
  flag.Set("log_dir", ".")
  flag.Set("v", "9")
  flag.Parse()

  glog.Info("Logging configured")
  
  ow := NewWorker()
  
  mc.WorkerRegister(ow)

  go mc.Init("./etc.tests/")
  time.Sleep(2 * time.Second)

  assert.Equal(t, "wasteout.ru", ow.GetAPI())
  assert.Equal(t, true, ow.ClientData.Status.Ok)

  r1 := mc.GetWorkersResults()
  res, _ := (r1["wasteout.ru"][""]).(mc.Result)
  assert.Equal(t, int64(2), res.Status.CntDevices)
  assert.Equal(t, int64(8), res.Status.CntMetrics)

}
