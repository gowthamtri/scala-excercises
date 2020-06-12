import codecs._

val res0 = Util.parseJson(""" { "name": "Bob", "age": 10 } """)

res0.flatMap(_.decodeAs[Person])

implicit def listDecoder[A](implicit decoder: Decoder[A]): Decoder[List[A]] = {

  Decoder.fromPartialFunction { case Json.Arr(as) => as.map(x => decoder.decode(x)) }

}

listDecoder(Json.Arr(List(Json.Str("a"), Json.Str("b"))))
