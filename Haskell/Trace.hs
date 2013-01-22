module Trace (traceJSON) where

import qualified Data.Aeson as JSON
import qualified Data.Text as Text

import Debug.Trace
import qualified Data.Text.Encoding as Text
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS


traceJSON :: (JSON.ToJSON a) => a -> b -> b
traceJSON subject result =
  trace (Text.unpack
          $ Text.decodeUtf8
          $ BS.concat
          $ LBS.toChunks
          $ JSON.encode subject)
        result
