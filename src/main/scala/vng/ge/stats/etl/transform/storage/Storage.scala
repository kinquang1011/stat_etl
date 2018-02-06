package vng.ge.stats.etl.transform.storage

import vng.ge.stats.etl.transform.storage.schema.Schema

/**
 * Created by tuonglv on 27/12/2016.
 */
abstract class Storage {
    var schema: Schema
    var outputPath: String
    var writeMode: String
}