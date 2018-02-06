package vng.ge.stats.etl.transform.adapter.base

import vng.ge.stats.etl.constant.Constants

/**
  * Created by canhtq on 30/03/2017.
  */
class FairySdkFormatter(_gameCode:String)  extends Formatter(_gameCode,"sdk") {
  setWarehouseDir(Constants.FAIRY_WAREHOUSE_DIR)


}

