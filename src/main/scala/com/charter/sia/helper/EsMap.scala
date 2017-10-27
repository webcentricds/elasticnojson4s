package com.charter.sia.helper

import org.apache.commons.lang3.StringUtils

class EsMap(val id: String,
            val map: Map[String, AnyRef]) {
  require(!StringUtils.isEmpty(id))
  require(map != null)
}
