module Database
  (Schema(..),
   createSchema,
   module Language.SQL.SQLite,
   UUID.UUID,
   Timestamp.Timestamp)
  where

import qualified Data.Binary as Binary
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS
import qualified Data.UUID as UUID
import qualified Timestamp as Timestamp

import Data.Maybe
import Language.SQL.SQLite


data Schema =
  Schema {
      schemaID :: UUID.UUID,
      schemaVersion :: Timestamp.Timestamp,
      schemaTables :: [CreateTable]
    }


createSchema :: Schema -> StatementList
createSchema schema = do
  StatementList
   $ concat [[Statement $ createTable "schema"
               [columnDefinition "schema_id" blobType,
                columnDefinition "version" integerType],
              Statement $ Insert InsertNoAlternative
               (SinglyQualifiedIdentifier Nothing "schema")
               (InsertValues [UnqualifiedIdentifier "schema_id",
                              UnqualifiedIdentifier "version"]
                             (fromJust $ mkOneOrMore
                               [ExpressionLiteralBlob
                                 $ (BS.concat . LBS.toChunks . Binary.encode)
                                   (schemaID schema),
                                ExpressionLiteralInteger
                                 $ fromIntegral (schemaVersion schema)]))],
             map Statement (schemaTables schema)]


createTable :: String -> [ColumnDefinition] -> CreateTable
createTable name columns =
  CreateTable NoTemporary NoIfNotExists
              (SinglyQualifiedIdentifier Nothing name)
              (ColumnsAndConstraints (fromJust $ mkOneOrMore columns) [])


primaryKeyColumnDefinition :: String -> Type -> ColumnDefinition
primaryKeyColumnDefinition name type' =
  ColumnDefinition (UnqualifiedIdentifier name) (JustType type')
   [ColumnPrimaryKey NoConstraintName NoAscDesc (Just OnConflictAbort)
                     NoAutoincrement]


columnDefinition :: String -> Type -> ColumnDefinition
columnDefinition name type' =
  ColumnDefinition (UnqualifiedIdentifier name) (JustType type') []


blobType :: Type
blobType =
  Type TypeAffinityNone
       (TypeName (fromJust $ mkOneOrMore [UnqualifiedIdentifier "blob"]))
       NoTypeSize


integerType :: Type
integerType =
  Type TypeAffinityNone
       (TypeName (fromJust $ mkOneOrMore [UnqualifiedIdentifier "integer"]))
       NoTypeSize
