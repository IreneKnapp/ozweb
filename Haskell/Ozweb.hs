module Main (main) where

import qualified Data.Aeson as Aeson
import qualified Data.ByteString.Lazy as LBS
import qualified Data.Text as Text
import qualified Data.Text.Encoding as Text
import qualified Data.Text.IO as Text
import qualified Language.SQL.SQLite as SQL
import qualified System.Environment as System

import qualified Database as D
import qualified Schema as S

import Data.String


main :: IO ()
main = do
  arguments <- System.getArgs
  case arguments of
    ["init", schemaFilename, databaseFilename] ->
      initialize schemaFilename databaseFilename
    _ -> help


help :: IO ()
help = do
  putStrLn "Usage:"
  putStrLn "ozweb init input.data-schema.json everything.db"


initialize :: FilePath -> FilePath -> IO ()
initialize schemaFilename databaseFilename = do
  schemaText <- Text.readFile schemaFilename
  case Aeson.eitherDecode' $ LBS.fromChunks [Text.encodeUtf8 schemaText] of
    Left message -> do
      putStrLn $ "Invalid schema: " ++ message
    Right schema -> do
      case S.compile schema of
        Left message -> do
          putStrLn message
          putStrLn "Unable to compile schema."
        Right schema -> do
          let statements = D.createSchema schema
          putStrLn $ show $ SQL.showTokens statements
