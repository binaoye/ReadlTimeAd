package nlp

import com.fasterxml.jackson.annotation.JsonProperty

class NameValue {

  @JsonProperty("name")
  var name: String = ""

  @JsonProperty("value")
  var value: Any = null

}
