{-# LANGUAGE OverloadedStrings, ExistentialQuantification,
             FlexibleInstances #-}
module Schema
  (Schema(..),
   EntitySpecification(..),
   ColumnSpecification(..),
   RelationSpecification(..),
   NameSpecification(..),
   NameSpecificationPart(..),
   TypeSpecification(..),
   compile)
  where

import qualified Control.Monad.Error as MTL
import qualified Control.Monad.Identity as MTL
import Data.Aeson ((.:), (.:?), (.!=), (.=))
import qualified Data.Aeson as JSON
import qualified Data.Aeson.Types as JSON
import qualified Data.HashMap.Strict as HashMap
import qualified Data.Map as Map
import qualified Data.Text as Text
import qualified Data.UUID as UUID
import qualified Database as D
import qualified Timestamp as Timestamp

import Control.Applicative
import Control.Monad
import Data.Function
import Data.HashMap.Strict (HashMap)
import Data.List
import Data.Map (Map)
import Data.Maybe
import Data.Monoid
import Data.Text (Text)

import Debug.Trace
import Trace


checkAllowedKeys :: [Text] -> HashMap Text JSON.Value -> JSON.Parser ()
checkAllowedKeys allowedKeys theMap = do
  mapM_ (\(key, _) -> do
            if elem key allowedKeys
              then return ()
              else fail $ "Unknown key " ++ show key ++ ".")
        (HashMap.toList theMap)


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
      schemaVersion :: Timestamp.Timestamp,
      schemaEntityTemplates :: Map String EntitySpecification,
      schemaEntities :: Map String EntitySpecification
    }
instance JSON.FromJSON Schema where
  parseJSON (JSON.Object value) = do
    checkAllowedKeys ["id",
                      "version",
                      "entity_templates",
                      "entities"]
                     value
    Schema <$> value .: "id"
           <*> value .: "version"
           <*> (value .:? "entity_templates" .!= Map.empty)
           <*> (value .:? "entities" .!= Map.empty)
  parseJSON _ = mzero


data EntitySpecification =
  EntitySpecification {
      entitySpecificationTemplate :: Maybe String,
      entitySpecificationVersioned :: Maybe Bool,
      entitySpecificationTimestamped :: Maybe Bool,
      entitySpecificationKey :: Maybe [ColumnSpecification],
      entitySpecificationColumns :: Maybe [ColumnSpecification],
      entitySpecificationRelations :: Maybe [RelationSpecification]
    }
instance JSON.FromJSON EntitySpecification where
  parseJSON (JSON.Object value) = do
    checkAllowedKeys ["template",
                      "versioned",
                      "timestamped",
                      "key",
                      "columns",
                      "relations"]
                     value
    EntitySpecification <$> value .:? "template"
                        <*> value .:? "versioned"
                        <*> value .:? "timestamped"
                        <*> value .:? "key"
                        <*> value .:? "columns"
                        <*> value .:? "relations"
  parseJSON _ = mzero
instance JSON.ToJSON EntitySpecification where
  toJSON entity =
    JSON.object $ concat
     [maybe [] (\value -> ["template" .= JSON.toJSON value])
       $ entitySpecificationTemplate entity,
      maybe [] (\value -> ["versioned" .= JSON.toJSON value])
       $ entitySpecificationVersioned entity,
      maybe [] (\value -> ["timestamped" .= JSON.toJSON value])
       $ entitySpecificationTimestamped entity,
      maybe [] (\value -> ["key" .= JSON.toJSON value])
       $ entitySpecificationKey entity,
      maybe [] (\value -> ["columns" .= JSON.toJSON value])
       $ entitySpecificationColumns entity,
      maybe [] (\value -> ["relations" .= JSON.toJSON value])
       $ entitySpecificationRelations entity]
instance Monoid EntitySpecification where
  mempty = EntitySpecification {
               entitySpecificationTemplate = Nothing,
               entitySpecificationVersioned = Nothing,
               entitySpecificationTimestamped = Nothing,
               entitySpecificationKey = Nothing,
               entitySpecificationColumns = Nothing,
               entitySpecificationRelations = Nothing
             }
  mappend a b =
    let replace
          :: (EntitySpecification -> Maybe a)
          -> Maybe a
        replace accessor = combine accessor (\a' b' -> b')
        concatenate
          :: (EntitySpecification -> Maybe [a])
          -> Maybe [a]
        concatenate accessor = combine accessor (\a' b' -> b' ++ a')
        useMonoid
          :: (Monoid a)
          => (EntitySpecification -> Maybe a)
          -> Maybe a
        useMonoid accessor = combine accessor mappend
        combine
          :: (EntitySpecification -> Maybe a)
          -> (a -> a -> a)
          -> Maybe a
        combine accessor combiner =
          case (accessor a, accessor b) of
            (Just a', Just b') -> Just $ combiner a' b'
            (Nothing, Just b') -> Just b'
            (Just a', Nothing) -> Just a'
            (Nothing, Nothing) -> Nothing
    in EntitySpecification {
           entitySpecificationTemplate =
             replace entitySpecificationTemplate,
           entitySpecificationVersioned =
             replace entitySpecificationVersioned,
           entitySpecificationTimestamped =
             replace entitySpecificationTimestamped,
           entitySpecificationKey =
             replace entitySpecificationKey,
           entitySpecificationColumns =
             concatenate entitySpecificationColumns,
           entitySpecificationRelations =
             concatenate entitySpecificationRelations
         }


data EntitySpecificationFlattened =
  EntitySpecificationFlattened {
      entitySpecificationFlattenedVersioned :: Bool,
      entitySpecificationFlattenedTimestamped :: Bool,
      entitySpecificationFlattenedKey :: [ColumnSpecification],
      entitySpecificationFlattenedColumns :: [ColumnSpecification],
      entitySpecificationFlattenedRelations :: [RelationSpecification]
    }
instance JSON.ToJSON EntitySpecificationFlattened where
  toJSON entity =
    JSON.object
     ["versioned" .= (JSON.toJSON
        $ entitySpecificationFlattenedVersioned entity),
      "timestamped" .= (JSON.toJSON
        $ entitySpecificationFlattenedTimestamped entity),
      "key" .= (JSON.toJSON
        $ entitySpecificationFlattenedKey entity),
      "columns" .= (JSON.toJSON
        $ entitySpecificationFlattenedColumns entity),
      "relations" .= (JSON.toJSON
        $ entitySpecificationFlattenedRelations entity)]


data ColumnSpecification =
  ColumnSpecification {
      columnSpecificationName :: NameSpecification,
      columnSpecificationType :: TypeSpecification,
      columnSpecificationConcretePathOf :: Maybe String
    }
instance JSON.FromJSON ColumnSpecification where
  parseJSON (JSON.Object value) = do
    checkAllowedKeys ["name",
                      "type",
                      "concrete_path_of"]
                     value
    ColumnSpecification <$> value .: "name"
                        <*> value .: "type"
                        <*> value .:? "concrete_path_of"
  parseJSON _ = mzero
instance JSON.ToJSON ColumnSpecification where
  toJSON column =
    JSON.object
      ["name" .= JSON.toJSON (columnSpecificationName column),
       "type" .= JSON.toJSON (columnSpecificationType column),
       "concrete_path_of" .= JSON.toJSON
         (columnSpecificationConcretePathOf column)]


data RelationSpecification =
  RelationSpecification {
      relationSpecificationEntity :: String,
      relationSpecificationPurpose :: Maybe String,
      relationSpecificationRequired :: Bool,
      relationSpecificationUnique :: Bool,
      relationSpecificationKey :: Maybe [NameSpecification]
    }
instance JSON.FromJSON RelationSpecification where
  parseJSON (JSON.Object value) = do
    checkAllowedKeys ["entity",
                      "purpose",
                      "required",
                      "unique",
                      "key"]
                     value
    purpose <- value .: "purpose"
    purpose <- case purpose of
                 JSON.Null -> return Nothing
                 JSON.String text -> return $ Just $ Text.unpack text
                 _ -> mzero
    RelationSpecification <$> value .: "entity"
                          <*> pure purpose
                          <*> value .: "required"
                          <*> value .: "unique"
                          <*> value .:? "key"
instance JSON.ToJSON RelationSpecification where
  toJSON relation =
    JSON.object
      ["entity" .= JSON.toJSON (relationSpecificationEntity relation),
       "purpose" .= JSON.toJSON (relationSpecificationPurpose relation),
       "required" .= JSON.toJSON (relationSpecificationRequired relation),
       "unique" .= JSON.toJSON (relationSpecificationUnique relation)]


data NameSpecification = NameSpecification [NameSpecificationPart]
instance JSON.FromJSON NameSpecification where
  parseJSON (JSON.String text) =
    pure $ NameSpecification [LiteralNameSpecificationPart $ Text.unpack text]
  parseJSON items@(JSON.Array _) =
    NameSpecification <$> JSON.parseJSON items
  parseJSON _ = mzero
instance JSON.ToJSON NameSpecification where
  toJSON (NameSpecification parts) = JSON.toJSON parts


data NameSpecificationPart
  = LiteralNameSpecificationPart String
  | VariableNameSpecificationPart String
instance JSON.FromJSON NameSpecificationPart where
  parseJSON (JSON.String text) =
    pure $ LiteralNameSpecificationPart $ Text.unpack text
  parseJSON (JSON.Object value) = do
    type' <- value .: "type"
    return (type' :: String)
    case type' of
      "constant" -> do
        checkAllowedKeys ["type",
                          "value"]
                         value
        LiteralNameSpecificationPart <$> value .: "value"
      "variable" -> do
        checkAllowedKeys ["type",
                          "name"]
                         value
        VariableNameSpecificationPart <$> value .: "name"
      _ -> mzero
  parseJSON _ = mzero
instance JSON.ToJSON NameSpecificationPart where
  toJSON (LiteralNameSpecificationPart value) =
    JSON.object
      ["type" .= JSON.toJSON ("constant" :: Text),
       "value" .= JSON.toJSON value]
  toJSON (VariableNameSpecificationPart name) =
    JSON.object
      ["type" .= JSON.toJSON ("variable" :: Text),
       "name" .= JSON.toJSON name]


data TypeSpecification = TypeSpecification String [TypeSpecification]
instance JSON.FromJSON TypeSpecification where
  parseJSON (JSON.String text) =
    pure $ TypeSpecification (Text.unpack text) []
  parseJSON items@(JSON.Array _) = do
    items <- JSON.parseJSON items
    case items of
      (constructor : parameters) -> do
        parameters <- JSON.parseJSON $ JSON.toJSON parameters
        return $ TypeSpecification constructor parameters
      _ -> mzero
  parseJSON _ = mzero
instance JSON.ToJSON TypeSpecification where
  toJSON (TypeSpecification constructor parameters) =
    let parameters' =
          fromJust $ JSON.parseMaybe JSON.parseJSON (JSON.toJSON parameters)
    in JSON.toJSON (constructor : parameters')


data Entity =
  Entity {
      entityName :: String,
      entityTables :: Map TableRole Table
    }
instance JSON.ToJSON Entity where
  toJSON entity =
    JSON.object
      ["name" .= JSON.toJSON (entityName entity),
       "tables" .= JSON.toJSON
         (Map.mapKeys (\role -> case role of
                                  MainTableRole -> "main" :: String
                                  VersionTableRole -> "version" :: String)
                      (entityTables entity))]


data Table =
  Table {
      tableName :: String,
      tableColumns :: Map String Column
    }
instance JSON.ToJSON Table where
  toJSON table =
    JSON.object
      ["name" .= JSON.toJSON (tableName table),
       "columns" .= JSON.toJSON (tableColumns table)]


data Column =
  Column {
      columnName :: String,
      columnType :: TypeSpecification,
      columnReadOnly :: Bool
    }
instance JSON.ToJSON Column where
  toJSON column =
    JSON.object
      ["name" .= JSON.toJSON (columnName column),
       "type" .= JSON.toJSON (columnType column),
       "read_only" .= JSON.toJSON (columnReadOnly column)]


data TableRole
  = MainTableRole
  | VersionTableRole
  deriving (Eq, Ord)
instance JSON.ToJSON TableRole where
  toJSON MainTableRole = JSON.toJSON ("main" :: Text)
  toJSON VersionTableRole = JSON.toJSON ("version" :: Text)


data PreColumn =
  PreColumn {
      preColumnName :: NameSpecification,
      preColumnType :: TypeSpecification,
      preColumnReadOnly :: Bool,
      preColumnTableRoles :: [TableRole],
      preColumnRole :: ColumnRole
    }
instance JSON.ToJSON PreColumn where
  toJSON column =
    JSON.object
      ["name" .= JSON.toJSON (preColumnName column),
       "type" .= JSON.toJSON (preColumnType column),
       "read_only" .= JSON.toJSON (preColumnReadOnly column),
       "table_roles" .= JSON.toJSON (preColumnTableRoles column),
       "roles" .= JSON.toJSON (preColumnRole column)]


data ColumnRole
  = KeyColumnRole Int
  | TimestampColumnRole
  | DataColumnRole
  deriving (Eq, Ord)
instance JSON.ToJSON ColumnRole where
  toJSON (KeyColumnRole index) =
    JSON.toJSON [JSON.toJSON ("key" :: Text), JSON.toJSON index]
  toJSON TimestampColumnRole = JSON.toJSON ("timestamp" :: Text)
  toJSON DataColumnRole = JSON.toJSON ("data" :: Text)


data Subcolumn =
  Subcolumn {
      subcolumnName :: NameSpecification,
      subcolumnType :: D.Type
    }
instance JSON.ToJSON Subcolumn where
  toJSON subcolumn =
    JSON.object
      ["name" .= JSON.toJSON (subcolumnName subcolumn),
       "type" .= JSON.toJSON (show $ D.showTokens $ subcolumnType subcolumn)]


class Type type' where
  typeSubcolumns :: type' -> [Subcolumn]


data AnyType = forall type' . Type type' => AnyType type'


data UUIDType = UUIDType
instance Type UUIDType where
  typeSubcolumns UUIDType =
    [Subcolumn {
         subcolumnName =
           NameSpecification [VariableNameSpecificationPart "column"],
         subcolumnType =
           D.Type D.TypeAffinityNone
                  (D.TypeName $ fromJust $ D.mkOneOrMore
                    [D.UnqualifiedIdentifier "blob"])
                  D.NoTypeSize
       }]


data TimestampType = TimestampType
instance Type TimestampType where
  typeSubcolumns TimestampType =
    [Subcolumn {
         subcolumnName =
           NameSpecification [VariableNameSpecificationPart "column"],
         subcolumnType =
           D.Type D.TypeAffinityNone
                  (D.TypeName $ fromJust $ D.mkOneOrMore
                    [D.UnqualifiedIdentifier "integer"])
                  D.NoTypeSize
       }]


data EmailType = EmailType
instance Type EmailType where
  typeSubcolumns EmailType =
    [Subcolumn {
        subcolumnName =
          NameSpecification [VariableNameSpecificationPart "column"],
        subcolumnType =
          D.Type D.TypeAffinityNone
                 (D.TypeName $ fromJust $ D.mkOneOrMore
                   [D.UnqualifiedIdentifier "text"])
                 D.NoTypeSize
      }]


data MaybeType = MaybeType AnyType
instance Type MaybeType where
  typeSubcolumns (MaybeType (AnyType subtype)) = typeSubcolumns subtype


data Compilation a = Compilation (MTL.ErrorT String MTL.Identity a)
instance Monad Compilation where
  return a = Compilation $ return a
  a >>= b = Compilation $ do
    let Compilation a' = a
    v <- a'
    let Compilation b' = b v
    b'


throwError :: String -> Compilation a
throwError message = Compilation $ do
  MTL.throwError message


tagErrors :: String -> Compilation a -> Compilation a
tagErrors message (Compilation action) = Compilation $ do
  MTL.catchError action
                 (\e -> MTL.throwError $ message ++ ": " ++ e)


runCompilation :: Compilation a -> Either String a
runCompilation (Compilation action) =
  MTL.runIdentity $ MTL.runErrorT action


compile :: Schema -> Either String D.Schema
compile schema = runCompilation $ do
  entities <- compileEntities (schemaEntityTemplates schema)
                              (schemaEntities schema)
  tables <- mapM compileTable
                 $ concatMap (\entity -> Map.elems $ entityTables entity)
                             (Map.elems entities)
  return $ D.Schema {
      D.schemaID = schemaID schema,
      D.schemaVersion = schemaVersion schema,
      D.schemaTables = tables
    }


compileEntities
  :: Map String EntitySpecification
  -> Map String EntitySpecification
  -> Compilation (Map String Entity)
compileEntities templates entities = do
  foldM (\soFar (name, entity) -> do
          tagErrors ("Compiling entity \"" ++ name ++ "\"") $ do
            flattened <- flattenEntitySpecification templates entity
            entity <- compileEntity name flattened soFar
            return $ Map.insert name entity soFar)
        Map.empty
        (Map.toList entities)


topologicallySortEntities
  :: Map String EntitySpecificationFlattened
  -> Compilation [EntitySpecificationFlattened]
topologicallySortEntities allEntities = do
  return []


compileEntity
  :: String
  -> EntitySpecificationFlattened
  -> Map String Entity
  -> Compilation Entity
compileEntity theEntityName flattened otherEntities = do
  let versioned = entitySpecificationFlattenedVersioned flattened
      timestamped = entitySpecificationFlattenedTimestamped flattened
      allTableRoles = if versioned
                        then [MainTableRole, VersionTableRole]
                        else [MainTableRole]
      basicKeyColumns =
        map (\(column, index) -> PreColumn {
                            preColumnName = columnSpecificationName column,
                            preColumnType = columnSpecificationType column,
                            preColumnReadOnly = True,
                            preColumnTableRoles = allTableRoles,
                            preColumnRole = KeyColumnRole index
                          })
            (zip (entitySpecificationFlattenedKey flattened) [0 ..])
      versionKeyColumn =
        PreColumn {
            preColumnName =
              NameSpecification [LiteralNameSpecificationPart "version_id"],
            preColumnType = TypeSpecification "uuid" [],
            preColumnReadOnly = True,
            preColumnTableRoles = [VersionTableRole],
            preColumnRole =
              KeyColumnRole
                (length $ entitySpecificationFlattenedKey flattened)
          }
      keyColumns =
        concat [basicKeyColumns,
                if versioned
                  then [versionKeyColumn]
                  else []]
      createdAtColumn =
        PreColumn {
            preColumnName =
              NameSpecification [LiteralNameSpecificationPart "created_at"],
            preColumnType = TypeSpecification "timestamp" [],
            preColumnReadOnly = True,
            preColumnTableRoles = [MainTableRole],
            preColumnRole = TimestampColumnRole
          }
      modifiedAtColumn =
        PreColumn {
            preColumnName =
              NameSpecification [LiteralNameSpecificationPart "modified_at"],
            preColumnType = TypeSpecification "timestamp" [],
            preColumnReadOnly = True,
            preColumnTableRoles =
              if versioned
                then [VersionTableRole]
                else [MainTableRole],
            preColumnRole = TimestampColumnRole
          }
      deletedAtColumn =
        PreColumn {
            preColumnName =
              NameSpecification [LiteralNameSpecificationPart "deleted_at"],
            preColumnType =
              TypeSpecification "maybe" [TypeSpecification "timestamp" []],
            preColumnReadOnly = True,
            preColumnTableRoles = [MainTableRole],
            preColumnRole = TimestampColumnRole
          }
      timestampColumns =
        if timestamped
          then [createdAtColumn, modifiedAtColumn, deletedAtColumn]
          else []
      dataColumns = []
      relationColumns = []
      allColumns =
        concat [keyColumns, timestampColumns, dataColumns, relationColumns]
      getColumnsForTableRole :: TableRole -> Compilation (Map String Column)
      getColumnsForTableRole tableRole = do
        mapM (\column -> do
                name <- substituteNameSpecification
                          (Map.fromList [("entity", theEntityName)])
                          (preColumnName column)
                        >>= finalizeNameSpecification
                return (name, Column {
                                 columnName = name,
                                 columnType = preColumnType column,
                                 columnReadOnly = preColumnReadOnly column
                               }))
             (sortBy (on compare preColumnRole)
                     $ filter (\column -> elem tableRole
                                               $ preColumnTableRoles column)
                              allColumns)
        >>= return . Map.fromList
      tableSpecifications =
        if versioned
          then [(theEntityName, MainTableRole),
                (theEntityName ++ "_version", VersionTableRole)]
          else [(theEntityName, MainTableRole)]
  tables <- mapM (\(name, role) -> do
                     columns <- getColumnsForTableRole role
                     let table = Table {
                                     tableName = name,
                                     tableColumns = columns
                                   }
                     return (role, table))
                 tableSpecifications
            >>= return . Map.fromList
  return $ Entity {
               entityName = theEntityName,
               entityTables = tables
             }


flattenEntitySpecification
  :: Map String EntitySpecification
  -> EntitySpecification
  -> Compilation EntitySpecificationFlattened
flattenEntitySpecification templates entity = do
  let getRelevantTemplates entity = do
        case entitySpecificationTemplate entity of
          Nothing -> return [entity]
          Just templateName ->
            case Map.lookup templateName templates of
              Nothing -> return [entity]
              Just template -> do
                rest <- getRelevantTemplates template
                return $ entity : rest
  relevantTemplates <- getRelevantTemplates entity >>= return . reverse
  let flattened = mconcat relevantTemplates
  versioned <- maybe (throwError "\"Versioned\" field undefined.")
                   return
                   (entitySpecificationVersioned flattened)
  timestamped <- maybe (throwError "\"Timestamped\" field undefined.")
                   return
                   (entitySpecificationTimestamped flattened)
  key <- maybe (throwError "\"Key\" field undefined.")
               return
               (entitySpecificationKey flattened)
  columns <- maybe (throwError "\"Columns\" field undefined.")
                   return
                   (entitySpecificationColumns flattened)
  relations <- maybe (throwError "\"Relations\" field undefined.")
                     return
                     (entitySpecificationRelations flattened)
  return $ EntitySpecificationFlattened {
               entitySpecificationFlattenedVersioned = versioned,
               entitySpecificationFlattenedTimestamped = timestamped,
               entitySpecificationFlattenedKey = key,
               entitySpecificationFlattenedColumns = columns,
               entitySpecificationFlattenedRelations = relations
            }


finalizeNameSpecification
  :: NameSpecification
  -> Compilation String
finalizeNameSpecification (NameSpecification parts) = do
  mapM (\part ->
          case part of
            LiteralNameSpecificationPart literal -> return literal
            VariableNameSpecificationPart variable ->
              throwError $ "Reference to undefined variable \"" ++ variable
                           ++ "\" in name specification.")
       parts
  >>= return . concat


substituteNameSpecification
  :: Map String String
  -> NameSpecification
  -> Compilation NameSpecification
substituteNameSpecification bindings (NameSpecification parts) = do
  parts <- mapM (\part -> do
                   case part of
                     VariableNameSpecificationPart variable -> do
                       case Map.lookup variable bindings of
                         Nothing -> return part
                         Just value ->
                           return $ LiteralNameSpecificationPart value
                     _ -> return part)
                parts
  return $ NameSpecification parts


compileTable :: Table -> Compilation D.CreateTable
compileTable table =
  tagErrors ("Compiling table \"" ++ (tableName table) ++ "\"") $ do
    columns <- mapM compileColumn (Map.elems $ tableColumns table)
               >>= return . concat
    case D.mkOneOrMore columns of
      Nothing -> throwError "No columns."
      Just columns ->
        return $ D.CreateTable D.NoTemporary
                   D.NoIfNotExists
                     (D.SinglyQualifiedIdentifier Nothing $ tableName table)
                     $ D.ColumnsAndConstraints columns []


compileColumn :: Column -> Compilation [D.ColumnDefinition]
compileColumn column = do
  AnyType type' <- compileType $ columnType column
  mapM (\subcolumn -> do
          name <- substituteNameSpecification
                    (Map.fromList [("column", columnName column)])
                    (subcolumnName subcolumn)
                  >>= finalizeNameSpecification
          return $ D.ColumnDefinition (D.UnqualifiedIdentifier name)
                                      (D.JustType $ subcolumnType subcolumn)
                                      [])
       (typeSubcolumns type')


compileType :: TypeSpecification -> Compilation AnyType
compileType (TypeSpecification "timestamp" []) = do
  return $ AnyType TimestampType
compileType (TypeSpecification "uuid" []) = do
  return $ AnyType UUIDType
compileType (TypeSpecification "email" []) = do
  return $ AnyType EmailType
compileType (TypeSpecification "maybe" [subtype]) = do
  subtype <- compileType subtype
  return $ AnyType (MaybeType subtype)
compileType (TypeSpecification name _) = do
  throwError $ "Unknown type \"" ++ name ++ "\"."

