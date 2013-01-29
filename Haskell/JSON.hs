module JSON
  ((.:), (.:?))
  where

import qualified Data.Aeson as Aeson
import qualified Data.Aeson.Types as Aeson
import qualified Data.Text as Text


(.:)
  :: Aeson.FromJSON a
  => Aeson.Object
  -> Text.Text
  -> Aeson.Parser a
object .: field = do
  case Aeson.parseEither (object Aeson..:) field of
    Left message -> fail $ "Parsing " ++ (Text.unpack field) ++ ": " ++ message
    Right result -> return result


(.:?)
  :: Aeson.FromJSON a
  => Aeson.Object
  -> Text.Text
  -> Aeson.Parser (Maybe a)
object .:? field = do
  case Aeson.parseEither (object Aeson..:?)  field of
    Left message -> fail $ "Parsing " ++ (Text.unpack field) ++ ": " ++ message
    Right result -> return result
