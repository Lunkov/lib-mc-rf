//  wasteout.go
// Get Data from  wasteout.ru
package wasteout

import (
  "fmt"
  "time"
  "strconv"
  "net/http"
  "io/ioutil"
  "encoding/json"
  "github.com/golang/glog"
  "github.com/google/uuid"
  "github.com/Lunkov/lib-gis"
  "github.com/Lunkov/lib-mc"
)

type WorkerInfo struct {
  mc.WorkerInfo
}

func NewWorker() *WorkerInfo {
  w := new(WorkerInfo)
  w.API = "wasteout.ru"
  return w
}

type CalcInfo struct {
  FullnessProcent   int        `json:"fullnessPercent"`
  ContainerCount    int        `json:"containerCount"`
  Active            bool       `json:"active"`
  Alarmed           bool       `json:"alarmed"`
  Broken            bool       `json:"broken"`
  LastMessage       int64      `json:"lastMessage"`
  LastVisit         int64      `json:"lastVisit"`
  LastFilled        int64      `json:"lastFilled"`
}

type PointInfo struct {
  Id            int        `json:"id"`
  Address       string     `json:"address"`
  AreaName      string     `json:"areaName"`
  AreaCode      string     `json:"areaCode"`
  GroupId       int        `json:"groupId"`
  GroupName     string     `json:"groupName"`
  Latitude      float64    `json:"latitude"`
  Longitude     float64    `json:"longitude"`
  Calc          CalcInfo   `json:"calc"`
}

type Info struct {
  Version  int         `json:"ver"`
  Status   int         `json:"status"`
  Points   []PointInfo `json:"points"`

  LastError   string   `json:"-"`
  Ok          bool     `json:"-"`
}

type DevInfo struct {
  ID                uuid.UUID  `json:"-"`
  DevId             int        `json:"-"`
  Address           string     `json:"address"`
  Latitude          float64    `json:"latitude"`
  Longitude         float64    `json:"longitude"`

  FullnessProcent   int        `json:"fullnessPercent"`
  ContainerCount    int        `json:"containerCount"`
  Active            bool       `json:"active"`
  Alarmed           bool       `json:"alarmed"`
  Broken            bool       `json:"broken"`
  LastMessage       int64      `json:"lastMessage"`
  LastVisit         int64      `json:"lastVisit"`
  LastFilled        int64      `json:"lastFilled"`

  Points      []PointInfo   `json:"points"`
}

func (w *WorkerInfo) httpGet() Info {
  var data Info
  resp, err := http.Get(w.ClientData.UrlState)
  if err != nil {
    w.ClientData.Status.Ok = false
    w.ClientData.Status.LastError = fmt.Sprintf("WasteOut: %s", err)
    glog.Errorf("ERR: %s\n", w.ClientData.Status.LastError)
    return data
  }
  defer resp.Body.Close()
  body, err := ioutil.ReadAll(resp.Body)
  if glog.V(9) {
    glog.Infof("DBG: WasteOut: body=%v\n", string(body))
  }
  if err := json.Unmarshal(body, &data); err != nil {
    w.ClientData.Status.Ok = false
    w.ClientData.Status.LastError = fmt.Sprintf("WasteOut: %s", err)
    glog.Errorf("ERR: %s\n", w.ClientData.Status.LastError)
    return data
  }
  if glog.V(9) {
    glog.Infof("DBG: WasteOut: %v\n", data)
  }
  w.ClientData.Status.Ok = true
  return data
}

func (w *WorkerInfo) GetData() {
  if glog.V(2) {
    glog.Infof("LOG: WasteOut started\n")
  }
    
  data := w.httpGet()
  if !w.ClientData.Status.Ok {
    return
  }
  // var tdev *type_device.Info
  /*
  tdev  = nil
  mdev := model_device.GetByCode(ClientData.DeviceModel_CODE)
  if mdev != nil {
    tdev  = type_device.GetById(mdev.TypeDevice_ID)
  }
  owner  := org.GetByCode(ClientData.Org_CODE)
  dplace := district.GetByCode(ClientData.District_CODE)
  */
  devs := calcDevDistance(data)
  var dm []mc.DeviceMetric
  for _, v := range devs {
    // dev := device.GetById(v.ID)
    w.ClientData.Status.CntDevices ++
    /*
    if dev == nil {
      var d device.Info
      code := fmt.Sprintf("%s.%v", api.DeviceModel_CODE, v.ID)
      d.ID = v.ID
      d.SN = code
      d.Name = fmt.Sprintf("Площадка ТБО %s", v.Address)
      d.Description = fmt.Sprintf("Площадка ТБО %s. %s", owner.Name, v.Address)
      d.Latitude  = v.Latitude
      d.Longitude = v.Longitude
      if tdev != nil {
        d.TypeDevice_ID    = tdev.ID
        d.TypeDevice_CODE  = tdev.CODE
        d.MonLayer_ID      = tdev.MonLayer_ID
        d.MonLayer_CODE    = tdev.MonLayer_CODE
        d.GisLayer_ID      = tdev.GisLayer_ID
        d.GisLayer_CODE    = tdev.GisLayer_CODE
      }
      if dplace != nil {
        d.District_ID, _  = uuid.Parse(api.District_ID)
        d.District_CODE   = api.District_CODE
      }
      if mdev != nil {
        d.ModelDevice_ID   = mdev.ID
        d.ModelDevice_CODE = mdev.CODE
      }
      if owner != nil {
        d.Owner_org_ID   = owner.ID
        d.Owner_org_CODE = owner.CODE
      }
      // SAVE
      if tdev != nil {
        glog.Infof("LOG: WasteOut New Device\n")
        newDev, _ := device.New(code, d)
        newDev.Update2DB()
        // DBUpdateItem(modelName string, x interface{}) 
      }
      dev = device.GetById(v.ID)
    } */
    dt := time.Unix(0, v.LastMessage * int64(time.Millisecond))
    
    // metering_value.SetValueByCode(dev.CODE, "Waste.Site.Containers.Count", dt, float64(v.ContainerCount))
    dm = append(dm, mc.DeviceMetric{Device_ID: v.ID, Metric_CODE: "Waste.Site.Containers.Count", DT: dt, Value: float64(v.ContainerCount)})
    // metering_value.SetValueByCode(dev.CODE, "Waste.Site.FullnessProcent",  dt, float64(v.FullnessProcent))
    dm = append(dm, mc.DeviceMetric{Device_ID: v.ID, Metric_CODE: "Waste.Site.FullnessProcent", DT: dt, Value: float64(v.FullnessProcent)})
    if v.Active {
      // metering_value.SetValueByCode(dev.CODE, "Waste.Site.Online",           dt, 1)
      dm = append(dm, mc.DeviceMetric{Device_ID: v.ID, Metric_CODE: "Waste.Site.Online", DT: dt, Value: float64(1)})
    } else {
      // metering_value.SetValueByCode(dev.CODE, "Waste.Site.Online",           dt, 0)
      dm = append(dm, mc.DeviceMetric{Device_ID: v.ID, Metric_CODE: "Waste.Site.Online", DT: dt, Value: float64(0)})
    }
    // metering_value.SetValueByCode(dev.CODE, "Waste.Site.Time.LastMessage", dt, float64(v.LastMessage))
    dm = append(dm, mc.DeviceMetric{Device_ID: v.ID, Metric_CODE: "Waste.Site.Time.LastMessage", DT: dt, Value: float64(v.LastMessage)})
    w.ClientData.Status.CntMetrics += 4
  }
  w.SendMetrics(&dm)
  if glog.V(2) {
    glog.Infof("LOG: WasteOut finished\n")
  }
  w.ClientData.Status.Ok = true
}

func calcDevDistance(devs Info) []DevInfo {
  var ardev []DevInfo

  for _, v := range devs.Points {
    ap := true
    for i, dev := range ardev {
      if dev.Address == v.Address {
        d := gis.Distance(v.Latitude, v.Longitude, dev.Latitude, dev.Longitude)
        if glog.V(9) {
          glog.Infof("DBG: WasteOut Calc GIS Distance (%s) Value=%f\n", v.Address, d)
        }
        if d < 50 {
          ardev[i].Points = append(dev.Points, v)
          ap = false
          break
        }
      }
    }
    if ap {
      item := DevInfo{Address: v.Address, Latitude: v.Latitude, Longitude: v.Longitude}
      item.Points = append(item.Points, v)
      ardev = append(ardev, item)
    }
  }
  for i, dev := range ardev {
    strID := dev.Address
    dev.Latitude = 0
    dev.Longitude = 0
    for _, v := range dev.Points {
      strID += strconv.Itoa(v.Id) + ":"
      dev.Latitude += v.Latitude
      dev.Longitude += v.Longitude
      dev.FullnessProcent += v.Calc.FullnessProcent
      if v.Calc.Alarmed {
        dev.Alarmed = true
      }
      if v.Calc.Broken {
        dev.Broken = true
      }
    }
    dev.ID = uuid.NewSHA1(uuid.Nil, ([]byte)(strID))
    dev.Latitude /= float64(len(dev.Points))
    dev.Longitude /= float64(len(dev.Points))
    dev.FullnessProcent /= len(dev.Points)
    ardev[i] = dev
  }
  return ardev
}
