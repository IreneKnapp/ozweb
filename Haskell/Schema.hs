{-# LANGUAGE OverloadedStrings #-}
module Schema
  (Schema(..),
   compile)
  where

import Data.Aeson ((.:))
import qualified Data.Aeson as JSON
import qualified Data.UUID as UUID
import qualified Data.Text as Text
import qualified Database as D
import qualified Timestamp as Timestamp

import Control.Applicative
import Control.Monad


instance JSON.FromJSON UUID.UUID where
  parseJSON (JSON.String text) =
    case reads $ Text.unpack text of
      [(uuid, "")] -> pure uuid
      _ -> mzero
  parseJSON _ = mzero


instance JSON.FromJSON Timestamp.Timestamp where
  parseJSON (JSON.String text) =
    case reads $ Text.unpack text of
      [(timestamp, "")] -> pure $ fromInteger timestamp
      _ -> mzero
  parseJSON _ = mzero


data Schema =
  Schema {
      schemaID :: UUID.UUID,
      schemaVersion :: Timestamp.Timestamp
    }
instance JSON.FromJSON Schema where
  parseJSON (JSON.Object value) =
    Schema <$> value .: "id"
           <*> value .: "version"
  parseJSON _ = mzero


compile :: Schema -> D.Schema
compile schema =
  D.Schema {
      D.schemaID = schemaID schema,
      D.schemaVersion = schemaVersion schema,
      D.schemaTables = []
    }
