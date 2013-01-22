{-# LANGUAGE OverloadedStrings, ExistentialQuantification,
             FlexibleInstances #-}
module Schema
  (Schema(..),
   EntitySpecification(..),
   HierarchySpecification(..),
   ColumnSpecification(..),
   RelatesToSpecification(..),
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
import qualified Data.Map as Map
import qualified Data.Text as Text
import qualified Data.UUID as UUID
import qualified Database as D
import qualified Timestamp as Timestamp

import Control.Applicative
import Control.Monad
import Data.Function
import Data.List
import Data.Map (Map)
import Data.Maybe
import Data.Monoid
import Data.Text (Text)

import Debug.Trace
import Trace


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
  parseJSON (JSON.Object value) =
    Schema <$> value .: "id"
           <*> value .: "version"
           <*> (value .:? "entity_templates" .!= Map.empty)
           <*> (value .:? "entities" .!= Map.empty)
  parseJSON _ = mzero


data EntitySpecification =
  EntitySpecification {
      entitySpecificationTemplate :: Maybe String,
      entitySpecificationExtends :: Maybe [String],
      entitySpecificationVersioned :: Maybe Bool,
      entitySpecificationTimestamped :: Maybe Bool,
      entitySpecificationHierarchy :: Maybe HierarchySpecification,
      entitySpecificationKey :: Maybe [ColumnSpecification],
      entitySpecificationColumns :: Maybe [ColumnSpecification],
      entitySpecificationRelatesTo :: Maybe [RelatesToSpecification]
    }
instance JSON.FromJSON EntitySpecification where
  parseJSON (JSON.Object value) =
    EntitySpecification <$> value .:? "template"
                        <*> value .:? "extends"
                        <*> value .:? "versioned"
                        <*> value .:? "timestamped"
                        <*> value .:? "hierarchy"
                        <*> value .:? "key"
                        <*> value .:? "columns"
                        <*> value .:? "relates_to"
  parseJSON _ = mzero
instance JSON.ToJSON EntitySpecification where
  toJSON entity =
    JSON.object $ concat
     [maybe [] (\value -> ["template" .= JSON.toJSON value])
       $ entitySpecificationTemplate entity,
      maybe [] (\value -> ["extends" .= JSON.toJSON value])
       $ entitySpecificationExtends entity,
      maybe [] (\value -> ["versioned" .= JSON.toJSON value])
       $ entitySpecificationVersioned entity,
      maybe [] (\value -> ["timestamped" .= JSON.toJSON value])
       $ entitySpecificationTimestamped entity,
      maybe [] (\value -> ["hierarchy" .= JSON.toJSON value])
       $ entitySpecificationHierarchy entity,
      maybe [] (\value -> ["key" .= JSON.toJSON value])
       $ entitySpecificationKey entity,
      maybe [] (\value -> ["columns" .= JSON.toJSON value])
       $ entitySpecificationColumns entity,
      maybe [] (\value -> ["relates_to" .= JSON.toJSON value])
       $ entitySpecificationRelatesTo entity]
instance Monoid EntitySpecification where
  mempty = EntitySpecification {
               entitySpecificationTemplate = Nothing,
               entitySpecificationExtends = Nothing,
               entitySpecificationVersioned = Nothing,
               entitySpecificationTimestamped = Nothing,
               entitySpecificationHierarchy = Nothing,
               entitySpecificationKey = Nothing,
               entitySpecificationColumns = Nothing,
               entitySpecificationRelatesTo = Nothing
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
           entitySpecificationExtends =
             concatenate entitySpecificationExtends,
           entitySpecificationVersioned =
             replace entitySpecificationVersioned,
           entitySpecificationTimestamped =
             replace entitySpecificationTimestamped,
           entitySpecificationHierarchy =
             useMonoid entitySpecificationHierarchy,
           entitySpecificationKey =
             replace entitySpecificationKey,
           entitySpecificationColumns =
             concatenate entitySpecificationColumns,
           entitySpecificationRelatesTo =
             concatenate entitySpecificationRelatesTo
         }


data EntitySpecificationFlattened =
  EntitySpecificationFlattened {
      entitySpecificationFlattenedExtends :: [String],
      entitySpecificationFlattenedVersioned :: Bool,
      entitySpecificationFlattenedTimestamped :: Bool,
      entitySpecificationFlattenedHierarchy :: HierarchySpecification,
      entitySpecificationFlattenedKey :: [ColumnSpecification],
      entitySpecificationFlattenedColumns :: [ColumnSpecification],
      entitySpecificationFlattenedRelatesTo :: [RelatesToSpecification]
    }
instance JSON.ToJSON EntitySpecificationFlattened where
  toJSON entity =
    JSON.object
     ["extends" .= (JSON.toJSON
        $ entitySpecificationFlattenedExtends entity),
      "versioned" .= (JSON.toJSON
        $ entitySpecificationFlattenedVersioned entity),
      "timestamped" .= (JSON.toJSON
        $ entitySpecificationFlattenedTimestamped entity),
      "hierarchy" .= (JSON.toJSON
        $ entitySpecificationFlattenedHierarchy entity),
      "key" .= (JSON.toJSON
        $ entitySpecificationFlattenedKey entity),
      "columns" .= (JSON.toJSON
        $ entitySpecificationFlattenedColumns entity),
      "relates_to" .= (JSON.toJSON
        $ entitySpecificationFlattenedRelatesTo entity)]


data HierarchySpecification
  = NoHierarchySpecification
  | HierarchySpecification {
        hierarchySpecificationPathColumns :: [String]
      }
instance JSON.FromJSON HierarchySpecification where
  parseJSON JSON.Null = pure $ NoHierarchySpecification
  parseJSON (JSON.Object value) =
    HierarchySpecification <$> value .: "path_columns"
  parseJSON _ = mzero
instance JSON.ToJSON HierarchySpecification where
  toJSON NoHierarchySpecification = JSON.Null
  toJSON hierarchy =
    JSON.object
      ["path_columns" .= JSON.toJSON
        (hierarchySpecificationPathColumns hierarchy)]
instance Monoid HierarchySpecification where
  mempty = NoHierarchySpecification
  mappend a@(HierarchySpecification {}) b@(HierarchySpecification { }) =
    HierarchySpecification {
        hierarchySpecificationPathColumns =
          hierarchySpecificationPathColumns b
          ++ hierarchySpecificationPathColumns b
      }
  mappend _ b = b



data ColumnSpecification =
  ColumnSpecification {
      columnSpecificationName :: NameSpecification,
      columnSpecificationType :: TypeSpecification
    }
instance JSON.FromJSON ColumnSpecification where
  parseJSON (JSON.Object value) =
    ColumnSpecification <$> value .: "name"
                        <*> value .: "type"
  parseJSON _ = mzero
instance JSON.ToJSON ColumnSpecification where
  toJSON column =
    JSON.object
      ["name" .= JSON.toJSON (columnSpecificationName column),
       "type" .= JSON.toJSON (columnSpecificationType column)]


data RelatesToSpecification =
  RelatesToSpecification {
      relatesToSpecificationEntity :: String,
      relatesToSpecificationPurpose :: Maybe String,
      relatesToSpecificationRequired :: Bool,
      relatesToSpecificationUnique :: Bool
    }
instance JSON.FromJSON RelatesToSpecification where
  parseJSON (JSON.Object value) = do
    purpose <- value .: "purpose"
    purpose <- case purpose of
                 JSON.Null -> return Nothing
                 JSON.String text -> return $ Just $ Text.unpack text
                 _ -> mzero
    RelatesToSpecification <$> value .: "entity"
                           <*> pure purpose
                           <*> value .: "required"
                           <*> value .: "unique"
instance JSON.ToJSON RelatesToSpecification where
  toJSON relation =
    JSON.object
      ["entity" .= JSON.toJSON (relatesToSpecificationEntity relation),
       "purpose" .= JSON.toJSON (relatesToSpecificationPurpose relation),
       "required" .= JSON.toJSON (relatesToSpecificationRequired relation),
       "unique" .= JSON.toJSON (relatesToSpecificationUnique relation)]


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
      "constant" -> LiteralNameSpecificationPart <$> value .: "value"
      "variable" -> VariableNameSpecificationPart <$> value .: "name"
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
                    [D.UnqualifiedIdentifier"integer"])
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


runCompilation :: Compilation a -> Either String a
runCompilation (Compilation action) =
  MTL.runIdentity $ MTL.runErrorT action


compile :: Schema -> Either String D.Schema
compile schema = runCompilation $ do
  let entities = computeEntities (schemaEntityTemplates schema)
                                 (schemaEntities schema)
  tables <- mapM compileTable
                 $ concatMap (\entity -> Map.elems $ entityTables entity)
                             (Map.elems entities)
  return $ traceJSON entities $ D.Schema {
      D.schemaID = schemaID schema,
      D.schemaVersion = schemaVersion schema,
      D.schemaTables = tables
    }


flattenEntitySpecification
  :: Map String EntitySpecification
  -> EntitySpecification
  -> EntitySpecificationFlattened
flattenEntitySpecification templates entity =
  let relevantTemplates =
        reverse
          $ unfoldr (\maybeEntity ->
                       case maybeEntity of
                         Nothing -> Nothing
                         Just entity ->
                           case entitySpecificationTemplate entity of
                             Nothing -> Just (entity, Nothing)
                             Just templateName ->
                               case Map.lookup templateName templates of
                                 Nothing -> Just (entity, Nothing)
                                 Just template ->
                                   Just (entity, Just template))
                    (Just entity)
      flattened = mconcat relevantTemplates
  in EntitySpecificationFlattened {
         entitySpecificationFlattenedExtends =
           fromMaybe [] $ entitySpecificationExtends flattened,
         entitySpecificationFlattenedVersioned =
           fromMaybe False $ entitySpecificationVersioned flattened,
         entitySpecificationFlattenedTimestamped =
           fromMaybe False $ entitySpecificationTimestamped flattened,
         entitySpecificationFlattenedHierarchy  =
           fromMaybe NoHierarchySpecification
            $ entitySpecificationHierarchy flattened,
         entitySpecificationFlattenedKey =
           fromMaybe [] $ entitySpecificationKey flattened,
         entitySpecificationFlattenedColumns =
           fromMaybe [] $ entitySpecificationColumns flattened,
         entitySpecificationFlattenedRelatesTo =
           fromMaybe [] $ entitySpecificationRelatesTo flattened
       }


computeEntity
  :: String
  -> EntitySpecificationFlattened
  -> Entity
computeEntity theEntityName flattened =
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
      allColumns =
        concat [keyColumns, timestampColumns, dataColumns]
      columnsForTableRole :: TableRole -> Map String Column
      columnsForTableRole tableRole =
        Map.fromList
          $ map (\column ->
                   let name =
                         finalizeNameSpecification $ substituteNameSpecification
                           (Map.fromList [("entity", theEntityName)])
                           (preColumnName column)
                   in (name, Column {
                                 columnName = name,
                                 columnType = preColumnType column,
                                 columnReadOnly = preColumnReadOnly column
                               }))
                $ sortBy (on compare preColumnRole)
                      $ filter (\column -> elem tableRole
                                                $ preColumnTableRoles column)
                               allColumns
      mainTable =
        Table {
            tableName = theEntityName,
            tableColumns = columnsForTableRole MainTableRole
          }
      versionTable =
        Table {
            tableName = theEntityName ++ "_version",
            tableColumns = columnsForTableRole VersionTableRole
          }
      tables = Map.fromList $ if versioned
                                then [(MainTableRole, mainTable),
                                      (VersionTableRole, versionTable)]
                                else [(MainTableRole, mainTable)]
  in Entity {
         entityName = theEntityName,
         entityTables = tables
       }


finalizeNameSpecification
  :: NameSpecification
  -> String
finalizeNameSpecification (NameSpecification parts) =
  concatMap (\part ->
               case part of
                 LiteralNameSpecificationPart literal -> literal
                 _ -> "")
            parts


substituteNameSpecification
  :: Map String String
  -> NameSpecification
  -> NameSpecification
substituteNameSpecification bindings (NameSpecification parts) =
  NameSpecification
    $ map (\part ->
             case part of
               VariableNameSpecificationPart variable ->
                 case Map.lookup variable bindings of
                   Nothing -> part
                   Just value -> LiteralNameSpecificationPart value
               _ -> part)
          parts


computeEntities
  :: Map String EntitySpecification
  -> Map String EntitySpecification
  -> Map String Entity
computeEntities templates entities =
  Map.mapWithKey
    (\name entity ->
       let flattened = flattenEntitySpecification templates entity
           entity' = computeEntity name flattened
       in entity')
    entities


compileTable :: Table -> Compilation D.CreateTable
compileTable table =
  let columns = concatMap computeColumn $ Map.elems $ tableColumns table
  in case columns of
       [] -> throwError "No columns."
       _ ->
         return $ D.CreateTable D.NoTemporary
                    D.NoIfNotExists
                      (D.SinglyQualifiedIdentifier Nothing $ tableName table)
                      $ D.ColumnsAndConstraints
                         (fromJust $ D.mkOneOrMore columns)
                         []


computeColumn :: Column -> [D.ColumnDefinition]
computeColumn column =
  map (\subcolumn ->
         D.ColumnDefinition
          (D.UnqualifiedIdentifier
            $ finalizeNameSpecification
                $ substituteNameSpecification
                    (Map.fromList [("column", columnName column)])
                    (subcolumnName subcolumn))
          (D.JustType $ subcolumnType subcolumn)
          [])
      (case computeType $ columnType column of
         AnyType type' -> typeSubcolumns type')


computeType :: TypeSpecification -> AnyType
computeType (TypeSpecification "timestamp" []) = AnyType TimestampType
computeType (TypeSpecification "uuid" []) = AnyType UUIDType
computeType (TypeSpecification "maybe" [subtype]) =
  AnyType (MaybeType $ computeType subtype)

