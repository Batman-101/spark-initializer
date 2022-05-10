package model.text

//Text data source supports only a single column, and should defined outside scope of serialization
case class TextDataMultiColumn(value: String, divisor: String, remainder: String)
