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
import qualified Data.Set as Set
import qualified Data.Text as Text
import qualified Data.UUID as UUID
import qualified Database as D
import qualified Language.SQL.SQLite as SQL
import qualified Timestamp as Timestamp

import Control.Applicative
import Control.Monad
import Data.Function
import Data.HashMap.Strict (HashMap)
import Data.List
import Data.Map (Map)
import Data.Maybe
import Data.Monoid
import Data.Set (Set)
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
instance JSON.ToJSON UUID.UUID where
  toJSON uuid = JSON.toJSON $ show uuid


instance JSON.FromJSON Timestamp.Timestamp where
  parseJSON (JSON.String text) =
    case reads $ Text.unpack text of
      [(timestamp, "")] -> pure $ fromInteger timestamp
      _ -> mzero
  parseJSON _ = mzero
instance JSON.ToJSON Timestamp.Timestamp where
  toJSON timestamp = JSON.toJSON $ show timestamp


data Schema =
  Schema {
      schemaID :: UUID.UUID,
      schemaVersion :: Timestamp.Timestamp,
      schemaTableRoles :: Map String TableRoleSpecification,
      schemaColumnFlags :: Set String,
      schemaColumnRoles :: Map String ColumnRoleSpecification,
      schemaColumnTemplates :: Map String ColumnSpecification,
      schemaEntityFlags :: Set String,
      schemaEntityTemplates :: Map String EntitySpecification,
      schemaEntities :: Map String EntitySpecification
    }
instance JSON.FromJSON Schema where
  parseJSON (JSON.Object value) = do
    checkAllowedKeys ["id",
                      "version",
                      "table_roles",
                      "column_flags",
                      "column_roles",
                      "column_templates",
                      "entity_flags",
                      "entity_templates",
                      "entities"]
                     value
    Schema <$> value .: "id"
           <*> value .: "version"
           <*> (value .:? "table_roles" .!= Map.empty)
           <*> (value .:? "column_flags" .!= Set.empty)
           <*> (value .:? "column_roles" .!= Map.empty)
           <*> (value .:? "column_templates" .!= Map.empty)
           <*> (value .:? "entity_flags" .!= [] >>= return . Set.fromList)
           <*> (value .:? "entity_templates" .!= Map.empty)
           <*> (value .:? "entities" .!= Map.empty)
  parseJSON _ = mzero
instance JSON.ToJSON Schema where
  toJSON schema =
    JSON.object
     ["id" .= (JSON.toJSON $ schemaID schema),
      "version" .= (JSON.toJSON $ schemaVersion schema),
      "table_roles" .= (JSON.toJSON $ schemaTableRoles schema),
      "column_flags" .= (JSON.toJSON $ Set.toList $ schemaColumnFlags schema),
      "column_roles" .= (JSON.toJSON $ schemaColumnRoles schema),
      "column_templates" .= (JSON.toJSON $ schemaColumnTemplates schema),
      "entity_flags" .= (JSON.toJSON $ Set.toList $ schemaEntityFlags schema),
      "entity_emplates" .= (JSON.toJSON $ schemaEntityTemplates schema),
      "entities" .= (JSON.toJSON $ schemaEntities schema)]


data TableRoleSpecification =
  TableRoleSpecification {
      tableRoleSpecificationName :: NameSpecification
    }
instance JSON.FromJSON TableRoleSpecification where
  parseJSON (JSON.Object value) = do
    checkAllowedKeys ["name"]
                     value
    TableRoleSpecification <$> value .: "name"
  parseJSON _ = mzero
instance JSON.ToJSON TableRoleSpecification where
  toJSON tableRole =
    JSON.object
     ["name" .= (JSON.toJSON
       $ tableRoleSpecificationName tableRole)]


data EntitySpecification =
  EntitySpecification {
      entitySpecificationTemplate :: Maybe String,
      entitySpecificationFlags :: Maybe (Map String Bool),
      entitySpecificationTables :: Maybe [String],
      entitySpecificationKey :: Maybe [ColumnSpecification],
      entitySpecificationColumns :: Maybe [ColumnSpecification],
      entitySpecificationRelations :: Maybe [RelationSpecification]
    }
instance JSON.FromJSON EntitySpecification where
  parseJSON (JSON.Object value) = do
    checkAllowedKeys ["template",
                      "flags",
                      "tables",
                      "key",
                      "columns",
                      "relations"]
                     value
    EntitySpecification <$> value .:? "template"
                        <*> value .:? "flags"
                        <*> value .:? "tables"
                        <*> value .:? "key"
                        <*> value .:? "columns"
                        <*> value .:? "relations"
  parseJSON _ = mzero
instance JSON.ToJSON EntitySpecification where
  toJSON entity =
    JSON.object $ concat
     [maybe [] (\value -> ["template" .= JSON.toJSON value])
       $ entitySpecificationTemplate entity,
      maybe [] (\value -> ["flags" .= JSON.toJSON value])
       $ entitySpecificationFlags entity,
      maybe [] (\value -> ["tables" .= JSON.toJSON value])
       $ entitySpecificationTables entity,
      maybe [] (\value -> ["key" .= JSON.toJSON value])
       $ entitySpecificationKey entity,
      maybe [] (\value -> ["columns" .= JSON.toJSON value])
       $ entitySpecificationColumns entity,
      maybe [] (\value -> ["relations" .= JSON.toJSON value])
       $ entitySpecificationRelations entity]
instance Monoid EntitySpecification where
  mempty = EntitySpecification {
               entitySpecificationTemplate = Nothing,
               entitySpecificationFlags = Nothing,
               entitySpecificationTables = Nothing,
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
        replaceKeys
          :: (EntitySpecification -> Maybe (Map String a))
          -> Maybe (Map String a)
        replaceKeys accessor = combine accessor (\a b -> Map.union b a)
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
           entitySpecificationFlags =
             replaceKeys entitySpecificationFlags,
           entitySpecificationTables =
             concatenate entitySpecificationTables,
           entitySpecificationKey =
             replace entitySpecificationKey,
           entitySpecificationColumns =
             concatenate entitySpecificationColumns,
           entitySpecificationRelations =
             concatenate entitySpecificationRelations
         }


data EntitySpecificationFlattened =
  EntitySpecificationFlattened {
      entitySpecificationFlattenedFlags :: Map String Bool,
      entitySpecificationFlattenedTables :: [String],
      entitySpecificationFlattenedKey :: [KeyColumnSpecificationFlattened],
      entitySpecificationFlattenedColumns :: [ColumnSpecificationFlattened],
      entitySpecificationFlattenedRelations :: [RelationSpecification]
    }
instance JSON.ToJSON EntitySpecificationFlattened where
  toJSON entity =
    JSON.object
     ["flags" .= (JSON.toJSON
        $ entitySpecificationFlattenedFlags entity),
      "tables" .= (JSON.toJSON
        $ entitySpecificationFlattenedTables entity),
      "key" .= (JSON.toJSON
        $ entitySpecificationFlattenedKey entity),
      "columns" .= (JSON.toJSON
        $ entitySpecificationFlattenedColumns entity),
      "relations" .= (JSON.toJSON
        $ entitySpecificationFlattenedRelations entity)]


data ColumnSpecification =
  ColumnSpecification {
      columnSpecificationTemplate :: Maybe String,
      columnSpecificationFlags :: Maybe (Map String Bool),
      columnSpecificationName :: Maybe NameSpecification,
      columnSpecificationType :: Maybe TypeSpecification,
      columnSpecificationTableRoles :: Maybe (Set String),
      columnSpecificationColumnRole :: Maybe String,
      columnSpecificationReadOnly :: Maybe Bool,
      columnSpecificationConcretePathOf :: Maybe String
    }
instance JSON.FromJSON ColumnSpecification where
  parseJSON (JSON.Object value) = do
    checkAllowedKeys ["template",
                      "flags",
                      "name",
                      "type",
                      "role",
                      "table_roles",
                      "read_only",
                      "concrete_path_of"]
                     value
    ColumnSpecification <$> value .:? "template"
                        <*> value .:? "flags"
                        <*> value .:? "name"
                        <*> value .:? "type"
                        <*> value .:? "role"
                        <*> value .:? "table_roles"
                        <*> value .:? "read_only"
                        <*> value .:? "concrete_path_of"
  parseJSON _ = mzero
instance JSON.ToJSON ColumnSpecification where
  toJSON column =
    JSON.object $ concat
     [maybe [] (\value -> ["template" .= JSON.toJSON value])
       $ columnSpecificationTemplate column,
      maybe [] (\value -> ["flags" .= JSON.toJSON value])
       $ columnSpecificationFlags column,
      maybe [] (\value -> ["name" .= JSON.toJSON value])
       $ columnSpecificationName column,
      maybe [] (\value -> ["type" .= JSON.toJSON value])
       $ columnSpecificationType column,
      maybe [] (\value -> ["role" .= JSON.toJSON value])
       $ columnSpecificationColumnRole column,
      maybe [] (\value -> ["table_roles" .= JSON.toJSON value])
       $ columnSpecificationTableRoles column,
      maybe [] (\value -> ["read_only" .= JSON.toJSON value])
       $ columnSpecificationReadOnly column,
      maybe [] (\value -> ["concrete_path_of" .= JSON.toJSON value])
       $ columnSpecificationConcretePathOf column]
instance Monoid ColumnSpecification where
  mempty = ColumnSpecification {
               columnSpecificationTemplate = Nothing,
               columnSpecificationFlags = Nothing,
               columnSpecificationName = Nothing,
               columnSpecificationType = Nothing,
               columnSpecificationTableRoles = Nothing,
               columnSpecificationColumnRole = Nothing,
               columnSpecificationReadOnly = Nothing,
               columnSpecificationConcretePathOf = Nothing
             }
  mappend a b =
    let replace
          :: (ColumnSpecification -> Maybe a)
          -> Maybe a
        replace accessor = combine accessor (\a' b' -> b')
        concatenate
          :: (ColumnSpecification -> Maybe [a])
          -> Maybe [a]
        concatenate accessor = combine accessor (\a' b' -> b' ++ a')
        replaceKeys
          :: (ColumnSpecification -> Maybe (Map String a))
          -> Maybe (Map String a)
        replaceKeys accessor = combine accessor (\a b -> Map.union b a)
        union accessor = combine accessor Set.union
        useMonoid
          :: (Monoid a)
          => (ColumnSpecification -> Maybe a)
          -> Maybe a
        useMonoid accessor = combine accessor mappend
        combine
          :: (ColumnSpecification -> Maybe a)
          -> (a -> a -> a)
          -> Maybe a
        combine accessor combiner =
          case (accessor a, accessor b) of
            (Just a', Just b') -> Just $ combiner a' b'
            (Nothing, Just b') -> Just b'
            (Just a', Nothing) -> Just a'
            (Nothing, Nothing) -> Nothing
    in ColumnSpecification {
           columnSpecificationTemplate =
             replace columnSpecificationTemplate,
           columnSpecificationFlags =
             replaceKeys columnSpecificationFlags,
           columnSpecificationName =
             replace columnSpecificationName,
           columnSpecificationType =
             replace columnSpecificationType,
           columnSpecificationTableRoles =
             union columnSpecificationTableRoles,
           columnSpecificationColumnRole =
             replace columnSpecificationColumnRole,
           columnSpecificationReadOnly =
             replace columnSpecificationReadOnly,
           columnSpecificationConcretePathOf =
             replace columnSpecificationConcretePathOf
         }


data KeyColumnSpecificationFlattened =
  KeyColumnSpecificationFlattened {
      keyColumnSpecificationFlattenedName :: NameSpecification,
      keyColumnSpecificationFlattenedType :: TypeSpecification,
      keyColumnSpecificationFlattenedTableRoles :: Set String,
      keyColumnSpecificationFlattenedColumnRole :: String
    }
instance JSON.ToJSON KeyColumnSpecificationFlattened where
  toJSON column =
    JSON.object
      ["name" .= keyColumnSpecificationFlattenedName column,
       "type" .= keyColumnSpecificationFlattenedType column,
       "table_roles" .=
         (Set.toList $ keyColumnSpecificationFlattenedTableRoles column),
       "column_role" .= keyColumnSpecificationFlattenedColumnRole column]


data ColumnSpecificationFlattened =
  ColumnSpecificationFlattened {
      columnSpecificationFlattenedName :: NameSpecification,
      columnSpecificationFlattenedType :: TypeSpecification,
      columnSpecificationFlattenedTableRoles :: Set String,
      columnSpecificationFlattenedColumnRole :: String,
      columnSpecificationFlattenedReadOnly :: Bool,
      columnSpecificationFlattenedConcretePathOf :: Maybe String
    }
instance JSON.ToJSON ColumnSpecificationFlattened where
  toJSON column =
    JSON.object $ concat
      [["name" .= columnSpecificationFlattenedName column,
        "type" .= columnSpecificationFlattenedType column,
        "table_roles" .=
          (Set.toList $ columnSpecificationFlattenedTableRoles column),
        "column_role" .= columnSpecificationFlattenedColumnRole column,
        "read_only" .= columnSpecificationFlattenedReadOnly column],
       maybe [] (\value -> ["concrete_path_of" .= value])
         (columnSpecificationFlattenedConcretePathOf column)]


data ColumnRoleSpecification =
  ColumnRoleSpecification {
      columnRoleSpecificationPriority :: Int
    }
instance JSON.FromJSON ColumnRoleSpecification where
  parseJSON (JSON.Object value) = do
    checkAllowedKeys ["priority"]
                     value
    ColumnRoleSpecification <$> value .: "priority"
  parseJSON _ = mzero
instance JSON.ToJSON ColumnRoleSpecification where
  toJSON columnRole =
    JSON.object
     ["priority" .= (JSON.toJSON
       $ columnRoleSpecificationPriority columnRole)]


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


data ConditionalSpecification content =
  ConditionalSpecification {
      conditionalSpecificationConditions :: [(Condition, content)]
    }
instance (JSON.FromJSON content)
         => JSON.FromJSON (ConditionalSpecification content) where
  parseJSON value@(JSON.Array _) = do
    items <- JSON.parseJSON value
    case items of
      (JSON.String case' : conditions) | case' == "case" -> do
        conditions <- mapM JSON.parseJSON conditions
        return $ ConditionalSpecification {
            conditionalSpecificationConditions = conditions
          }
      _ -> JSON.parseJSON value
  parseJSON _ = mzero
instance (JSON.ToJSON content)
         => JSON.ToJSON (ConditionalSpecification content) where
  toJSON conditional = do
    JSON.toJSON (JSON.String "case",
                 conditionalSpecificationConditions conditional)


data ConditionalListSpecification content =
  ConditionalListSpecification {
      conditionalListSpecificationItems :: [(Condition, [content])]
    }
instance (JSON.FromJSON content)
         => JSON.FromJSON (ConditionalListSpecification content) where
  parseJSON value@(JSON.Array _) = do
    subvalues <- JSON.parseJSON value
    mapM (\subvalue -> do
             case JSON.parseMaybe JSON.parseJSON subvalue of
               Just (JSON.String when' : condition : items)
                 | when' == "when" -> do
                   condition <- JSON.parseJSON condition
                   items <- mapM JSON.parseJSON items
                   return (condition, items)
               _ -> do
                 item <- JSON.parseJSON subvalue
                 return (DefaultCondition, [item]))
         subvalues
    >>= return . ConditionalListSpecification
  parseJSON _ = mzero
instance (JSON.ToJSON content)
         => JSON.ToJSON (ConditionalListSpecification content) where
  toJSON conditional =
    JSON.toJSON $ map (\(condition, items) ->
                          JSON.toJSON (JSON.String "when"
                                       : JSON.toJSON condition
                                       : map JSON.toJSON items))
                      (conditionalListSpecificationItems conditional)


data Condition
  = Condition {
        conditionFlags :: Map String Bool
      }
  | DefaultCondition
instance JSON.FromJSON Condition where
  parseJSON (JSON.String text)
    | text == "default" = do
        pure DefaultCondition
    | otherwise = do
        pure $ Condition {
                   conditionFlags = Map.fromList [(Text.unpack text, True)]
                 }
  parseJSON values@(JSON.Array _) = do
    items <- JSON.parseJSON values
    case items of
      (JSON.String not', JSON.String text)
        | not' == "not" -> do
            pure $ Condition {
                       conditionFlags =
                         Map.fromList [(Text.unpack text, False)]
                     }
      _ -> mzero
  parseJSON items@(JSON.Object _) = do
    JSON.parseJSON items >>= return . Condition . Map.fromList
  parseJSON _ = mzero
instance JSON.ToJSON Condition where
  toJSON condition@(Condition { }) = do
    JSON.toJSON $ conditionFlags condition
  toJSON DefaultCondition = do
    JSON.String "default"


data TableRole =
  TableRole {
      tableRoleName :: String,
      tableRoleTableName :: NameSpecification
    }
instance JSON.ToJSON TableRole where
  toJSON tableRole =
    JSON.object
     ["name" .= (JSON.toJSON $ tableRoleName tableRole),
      "table_name" .= (JSON.toJSON $ tableRoleTableName tableRole)]


data Entity =
  Entity {
      entityName :: String,
      entityTables :: Map String Table
    }
instance JSON.ToJSON Entity where
  toJSON entity =
    JSON.object
      ["name" .= JSON.toJSON (entityName entity),
       "tables" .= JSON.toJSON (entityTables entity)]


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


data PreEntity =
  PreEntity {
      preEntityName :: String,
      preEntityFlags :: Map String Bool,
      preEntityTableRoles :: Map String TableRole,
      preEntityColumnRoles :: Map String ColumnRole,
      preEntityKeyColumns :: [PreColumn],
      preEntityDataColumns :: [ColumnSpecificationFlattened],
      preEntityRelations :: [RelationSpecification]
    }
instance JSON.ToJSON PreEntity where
  toJSON entity =
    JSON.object
      ["name" .= JSON.toJSON (preEntityName entity),
       "flags" .= JSON.toJSON (preEntityFlags entity),
       "table_roles" .= JSON.toJSON (preEntityTableRoles entity),
       "column_roles" .= JSON.toJSON (preEntityColumnRoles entity),
       "key_columns" .= JSON.toJSON (preEntityKeyColumns entity),
       "data_columns" .= JSON.toJSON (preEntityDataColumns entity),
       "relations" .= JSON.toJSON (preEntityRelations entity)]


data PreColumn =
  PreColumn {
      preColumnName :: NameSpecification,
      preColumnType :: TypeSpecification,
      preColumnReadOnly :: Bool,
      preColumnTableRoles :: Set String,
      preColumnRole :: ColumnRole,
      preColumnConcretePathOf :: Maybe String
    }
instance JSON.ToJSON PreColumn where
  toJSON column =
    JSON.object
      ["name" .= JSON.toJSON (preColumnName column),
       "type" .= JSON.toJSON (preColumnType column),
       "read_only" .= JSON.toJSON (preColumnReadOnly column),
       "table_roles" .= JSON.toJSON (Set.toList $ preColumnTableRoles column),
       "roles" .= JSON.toJSON (preColumnRole column),
       "concrete_path_of" .= JSON.toJSON (preColumnConcretePathOf column)]


data ColumnRole =
  ColumnRole {
      columnRoleName :: String,
      columnRolePriority :: Int
    }
instance JSON.ToJSON ColumnRole where
  toJSON columnRole =
    JSON.object
     ["name" .= (JSON.toJSON $ columnRoleName columnRole),
      "priority" .= (JSON.toJSON $ columnRolePriority columnRole)]


data Subcolumn =
  Subcolumn {
      subcolumnName :: NameSpecification,
      subcolumnType :: SQL.Type
    }
instance JSON.ToJSON Subcolumn where
  toJSON subcolumn =
    JSON.object
      ["name" .= JSON.toJSON (subcolumnName subcolumn),
       "type" .= JSON.toJSON (show $ SQL.showTokens $ subcolumnType subcolumn)]


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
           SQL.Type SQL.TypeAffinityNone
                    (SQL.TypeName $ fromJust $ SQL.mkOneOrMore
                    [SQL.UnqualifiedIdentifier "blob"])
                    SQL.NoTypeSize
       }]


data TimestampType = TimestampType
instance Type TimestampType where
  typeSubcolumns TimestampType =
    [Subcolumn {
         subcolumnName =
           NameSpecification [VariableNameSpecificationPart "column"],
         subcolumnType =
           SQL.Type SQL.TypeAffinityNone
                    (SQL.TypeName $ fromJust $ SQL.mkOneOrMore
                    [SQL.UnqualifiedIdentifier "integer"])
                    SQL.NoTypeSize
       }]


data EmailType = EmailType
instance Type EmailType where
  typeSubcolumns EmailType =
    [Subcolumn {
        subcolumnName =
          NameSpecification [VariableNameSpecificationPart "column"],
        subcolumnType =
          SQL.Type SQL.TypeAffinityNone
                   (SQL.TypeName $ fromJust $ SQL.mkOneOrMore
                   [SQL.UnqualifiedIdentifier "text"])
                   SQL.NoTypeSize
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
  tableRoles <- compileTableRoles (schemaTableRoles schema)
  columnRoles <- compileColumnRoles (schemaColumnRoles schema)
  entities <- compileEntities tableRoles
                              columnRoles
                              (schemaEntityFlags schema)
                              (schemaEntityTemplates schema)
                              (schemaColumnFlags schema)
                              (schemaColumnTemplates schema)
                              (schemaEntities schema)
  tables <- mapM compileTable
                 $ concatMap (\entity -> Map.elems $ entityTables entity)
                             (Map.elems entities)
  return $ D.Schema {
      D.schemaID = schemaID schema,
      D.schemaVersion = schemaVersion schema,
      D.schemaTables = tables
    }


compileTableRoles
  :: Map String TableRoleSpecification
  -> Compilation (Map String TableRole)
compileTableRoles tableRoles = do
  mapM (\(name, tableRole) -> do
           tableRole <- compileTableRole name tableRole
           return (name, tableRole))
       (Map.toList tableRoles)
  >>= return . Map.fromList


compileTableRole :: String -> TableRoleSpecification -> Compilation TableRole
compileTableRole name tableRole = do
  let tableName = tableRoleSpecificationName tableRole
  return $ TableRole {
               tableRoleName = name,
               tableRoleTableName = tableName
             }


compileColumnRoles
  :: Map String ColumnRoleSpecification
  -> Compilation (Map String ColumnRole)
compileColumnRoles columnRoles = do
  mapM (\(name, columnRole) -> do
           columnRole <- compileColumnRole name columnRole
           return (name, columnRole))
       (Map.toList columnRoles)
  >>= return . Map.fromList


compileColumnRole :: String -> ColumnRoleSpecification -> Compilation ColumnRole
compileColumnRole name columnRole = do
  let columnPriority = columnRoleSpecificationPriority columnRole
  return $ ColumnRole {
               columnRoleName = name,
               columnRolePriority = columnPriority
             }


compileEntities
  :: Map String TableRole
  -> Map String ColumnRole
  -> Set String
  -> Map String EntitySpecification
  -> Set String
  -> Map String ColumnSpecification
  -> Map String EntitySpecification
  -> Compilation (Map String Entity)
compileEntities
    allTableRoles
    allColumnRoles
    allEntityFlags entityTemplates
    allColumnFlags columnTemplates
    entities = do
  firstPass <-
    foldM (\soFar (name, entity) -> do
             tagErrors ("Pre-compiling entity \"" ++ name ++ "\"") $ do
               flattened <-
                 flattenEntitySpecification allEntityFlags entityTemplates
                                            allColumnFlags columnTemplates
                                            entity
               entity <- preCompileEntity allTableRoles allColumnRoles
                                          name flattened
               return $ Map.insert name entity soFar)
          Map.empty
          (Map.toList entities)
  foldM (\soFar (name, entity) -> do
           tagErrors ("Compiling entity \"" ++ name ++ "\"") $ do
           entity <- compileEntity entity firstPass
           return $ Map.insert name entity soFar)
        Map.empty
        (Map.toList firstPass)


preCompileEntity
  :: Map String TableRole
  -> Map String ColumnRole
  -> String
  -> EntitySpecificationFlattened
  -> Compilation PreEntity
preCompileEntity allTableRoles allColumnRoles theEntityName flattened = do
  relevantTableRoles <-
    mapM (\theTableRoleName -> do
            case Map.lookup theTableRoleName allTableRoles of
              Nothing -> throwError $ "Undefined table role \""
                                      ++ theTableRoleName ++ "\"."
              Just tableRole -> return (theTableRoleName, tableRole))
         (entitySpecificationFlattenedTables flattened)
    >>= return . Map.fromList
  keyColumns <-
    mapM (\column -> do
            let name = keyColumnSpecificationFlattenedName column
                type' = keyColumnSpecificationFlattenedType column
                theColumnRoleName =
                  keyColumnSpecificationFlattenedColumnRole column 
                tableRoles = keyColumnSpecificationFlattenedTableRoles column
            columnRole <-
              case Map.lookup theColumnRoleName allColumnRoles of
                Nothing -> throwError
                  $ "Undefined column role \"" ++ theColumnRoleName ++ "\"."
                Just columnRole -> return columnRole
            return $ PreColumn {
                         preColumnName = name,
                         preColumnType = type',
                         preColumnTableRoles = tableRoles,
                         preColumnRole = columnRole,
                         preColumnReadOnly = True,
                         preColumnConcretePathOf = Nothing
                       })
         (entitySpecificationFlattenedKey flattened)
  let flags = entitySpecificationFlattenedFlags flattened
      dataColumns = entitySpecificationFlattenedColumns flattened
      relations = entitySpecificationFlattenedRelations flattened
  return $ PreEntity {
               preEntityName = theEntityName,
               preEntityFlags = flags,
               preEntityTableRoles = relevantTableRoles,
               preEntityColumnRoles = allColumnRoles,
               preEntityKeyColumns = keyColumns,
               preEntityDataColumns = dataColumns,
               preEntityRelations = relations
             }


compileEntity
  :: PreEntity
  -> Map String PreEntity
  -> Compilation Entity
compileEntity preEntity preEntities = do
  let substitutionVariables :: Map String String
      substitutionVariables =
        Map.fromList [("entity", preEntityName preEntity)]
      allColumnRoles = preEntityColumnRoles preEntity
  dataColumns <-
    mapM (\column -> do
            let name = columnSpecificationFlattenedName column
                type' = columnSpecificationFlattenedType column
                tableRoles = columnSpecificationFlattenedTableRoles column
                theColumnRoleName =
                  columnSpecificationFlattenedColumnRole column
                readOnly = columnSpecificationFlattenedReadOnly column
                concretePathOf =
                  columnSpecificationFlattenedConcretePathOf column
            columnRole <-
              case Map.lookup theColumnRoleName allColumnRoles of
                Nothing -> throwError
                  $ "Undefined column role \"" ++ theColumnRoleName ++ "\"."
                Just columnRole -> return columnRole
            return $ PreColumn {
                         preColumnName = name,
                         preColumnType = type',
                         preColumnTableRoles = tableRoles,
                         preColumnRole = columnRole,
                         preColumnReadOnly = readOnly,
                         preColumnConcretePathOf = concretePathOf
                       })
         (preEntityDataColumns preEntity)
  let allColumns =
        concat [preEntityKeyColumns preEntity, dataColumns]
      relevantTableRoles = preEntityTableRoles preEntity
      getColumnsForTableRole :: TableRole -> Compilation (Map String Column)
      getColumnsForTableRole tableRole = do
        mapM (\column -> do
                name <- substituteNameSpecification
                          substitutionVariables
                          (preColumnName column)
                        >>= finalizeNameSpecification
                return (name, Column {
                                 columnName = name,
                                 columnType = preColumnType column,
                                 columnReadOnly = preColumnReadOnly column
                               }))
             (sortBy (on compare (columnRolePriority . preColumnRole))
                     $ filter (\column ->
                                 Set.member (tableRoleName tableRole)
                                            (preColumnTableRoles column))
                              allColumns)
        >>= return . Map.fromList
  tables <- mapM (\tableRole -> do
                     columns <- getColumnsForTableRole tableRole
                     name <- substituteNameSpecification
                               substitutionVariables
                               (tableRoleTableName tableRole)
                             >>= finalizeNameSpecification
                     let table = Table {
                                     tableName = name,
                                     tableColumns = columns
                                   }
                     return (tableRoleName tableRole, table))
                 (Map.elems $ preEntityTableRoles preEntity)
            >>= return . Map.fromList
  return $ Entity {
               entityName = preEntityName preEntity,
               entityTables = tables
             }


flattenEntitySpecification
  :: Set String
  -> Map String EntitySpecification
  -> Set String
  -> Map String ColumnSpecification
  -> EntitySpecification
  -> Compilation EntitySpecificationFlattened
flattenEntitySpecification
    allFlags templates allColumnFlags columnTemplates entity = do
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
  flags <- maybe (throwError "\"Flags\" field undefined.")
                 return
                 (entitySpecificationFlags flattened)
  let presentFlags = Set.fromList $ map fst $ Map.toList flags
      unknownFlags = Set.difference presentFlags allFlags
  if not $ Set.null unknownFlags
    then throwError $ "Unknown flags "
                      ++ (intercalate ", " $ map show
                           $ Set.toList unknownFlags)
                      ++ "."
    else return ()
  let missingFlags = Set.difference allFlags presentFlags 
  if not $ Set.null missingFlags
    then throwError $ "Missing flags "
                      ++ (intercalate ", " $ map show
                           $ Set.toList missingFlags)
                      ++ "."
    else return ()
  tables <- maybe (throwError "\"Tables\" field undefined.")
                  return
                  (entitySpecificationTables flattened)
  if null tables
    then throwError "Entity has no tables."
    else return ()
  key <- maybe (throwError "\"Key\" field undefined.")
               return
               (entitySpecificationKey flattened)
  key <-
    mapM (flattenKeyColumnSpecification allColumnFlags columnTemplates)
          key
  columns <- maybe (throwError "\"Columns\" field undefined.")
                   return
                   (entitySpecificationColumns flattened)
  columns <-
    mapM (flattenColumnSpecification allColumnFlags columnTemplates)
         columns
  relations <- maybe (throwError "\"Relations\" field undefined.")
                     return
                     (entitySpecificationRelations flattened)
  return $ EntitySpecificationFlattened {
               entitySpecificationFlattenedFlags = flags,
               entitySpecificationFlattenedTables = tables,
               entitySpecificationFlattenedKey = key,
               entitySpecificationFlattenedColumns = columns,
               entitySpecificationFlattenedRelations = relations
            }


flattenKeyColumnSpecification
  :: Set String
  -> Map String ColumnSpecification
  -> ColumnSpecification
  -> Compilation KeyColumnSpecificationFlattened
flattenKeyColumnSpecification allFlags templates column = do
  let getRelevantTemplates column = do
        case columnSpecificationTemplate column of
          Nothing -> return [column]
          Just templateName ->
            case Map.lookup templateName templates of
              Nothing -> return [column]
              Just template -> do
                rest <- getRelevantTemplates template
                return $ column : rest
  relevantTemplates <- getRelevantTemplates column >>= return . reverse
  let flattened = mconcat relevantTemplates
  name <- maybe (throwError "\"Name\" field undefined.")
                return
                (columnSpecificationName flattened)
  type' <- maybe (throwError "\"Type\" field undefined.")
                return
                (columnSpecificationType flattened)
  flags <- maybe (throwError "\"Flags\" field undefined.")
                 return
                 (columnSpecificationFlags flattened)
  let presentFlags = Set.fromList $ map fst $ Map.toList flags
      unknownFlags = Set.difference presentFlags allFlags
  if not $ Set.null unknownFlags
    then throwError $ "Unknown flags "
                      ++ (intercalate ", " $ map show
                           $ Set.toList unknownFlags)
                      ++ "."
    else return ()
  let missingFlags = Set.difference allFlags presentFlags 
  if not $ Set.null missingFlags
    then throwError $ "Missing flags "
                      ++ (intercalate ", " $ map show
                           $ Set.toList missingFlags)
                      ++ "."
    else return ()
  tableRoles <- maybe (throwError "\"Table roles\" field undefined.")
                      return
                      (columnSpecificationTableRoles flattened)
  if Set.null tableRoles
    then throwError "Column has no table roles."
    else return ()
  columnRole <- maybe (throwError "\"Column role\" field undefined.")
                      return
                      (columnSpecificationColumnRole flattened)
  case columnSpecificationReadOnly flattened of
    Nothing -> return ()
    Just _ -> throwError "\"Read-only\" field defined on a key column."
  case columnSpecificationConcretePathOf flattened of
    Nothing -> return ()
    Just _ -> throwError "\"Concrete-path-of\" field defined on a key column."
  return $ KeyColumnSpecificationFlattened {
               keyColumnSpecificationFlattenedName = name,
               keyColumnSpecificationFlattenedType = type',
               keyColumnSpecificationFlattenedTableRoles = tableRoles,
               keyColumnSpecificationFlattenedColumnRole = columnRole
             }


flattenColumnSpecification
  :: Set String
  -> Map String ColumnSpecification
  -> ColumnSpecification
  -> Compilation ColumnSpecificationFlattened
flattenColumnSpecification allFlags templates column = do
  let getRelevantTemplates column = do
        case columnSpecificationTemplate column of
          Nothing -> return [column]
          Just templateName ->
            case Map.lookup templateName templates of
              Nothing -> return [column]
              Just template -> do
                rest <- getRelevantTemplates template
                return $ column : rest
  relevantTemplates <- getRelevantTemplates column >>= return . reverse
  let flattened = mconcat relevantTemplates
  name <- maybe (throwError "\"Name\" field undefined.")
                return
                (columnSpecificationName flattened)
  type' <- maybe (throwError "\"Type\" field undefined.")
                return
                (columnSpecificationType flattened)
  flags <- maybe (throwError "\"Flags\" field undefined.")
                 return
                 (columnSpecificationFlags flattened)
  let presentFlags = Set.fromList $ map fst $ Map.toList flags
      unknownFlags = Set.difference presentFlags allFlags
  if not $ Set.null unknownFlags
    then throwError $ "Unknown flags "
                      ++ (intercalate ", " $ map show
                           $ Set.toList unknownFlags)
                      ++ "."
    else return ()
  let missingFlags = Set.difference allFlags presentFlags 
  if not $ Set.null missingFlags
    then throwError $ "Missing flags "
                      ++ (intercalate ", " $ map show
                           $ Set.toList missingFlags)
                      ++ "."
    else return ()
  tableRoles <- maybe (throwError "\"Table roles\" field undefined.")
                      return
                      (columnSpecificationTableRoles flattened)
  if Set.null tableRoles
    then throwError "Column has no table roles."
    else return ()
  columnRole <- maybe (throwError "\"Column role\" field undefined.")
                      return
                      (columnSpecificationColumnRole flattened)
  readOnly <- maybe (throwError "\"Read-only\" field undefined.")
                    return
                    (columnSpecificationReadOnly flattened)
  let concretePathOf = columnSpecificationConcretePathOf flattened
  return $ ColumnSpecificationFlattened {
               columnSpecificationFlattenedName = name,
               columnSpecificationFlattenedType = type',
               columnSpecificationFlattenedTableRoles = tableRoles,
               columnSpecificationFlattenedColumnRole = columnRole,
               columnSpecificationFlattenedReadOnly = readOnly,
               columnSpecificationFlattenedConcretePathOf = concretePathOf
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


compileTable :: Table -> Compilation SQL.CreateTable
compileTable table =
  tagErrors ("Compiling table \"" ++ (tableName table) ++ "\"") $ do
    columns <- mapM compileColumn (Map.elems $ tableColumns table)
               >>= return . concat
    case SQL.mkOneOrMore columns of
      Nothing -> throwError "No columns."
      Just columns ->
        return $ SQL.CreateTable SQL.NoTemporary
                   SQL.NoIfNotExists
                     (SQL.SinglyQualifiedIdentifier Nothing $ tableName table)
                     $ SQL.ColumnsAndConstraints columns []


compileColumn :: Column -> Compilation [SQL.ColumnDefinition]
compileColumn column = do
  AnyType type' <- compileType $ columnType column
  mapM (\subcolumn -> do
          name <- substituteNameSpecification
                    (Map.fromList [("column", columnName column)])
                    (subcolumnName subcolumn)
                  >>= finalizeNameSpecification
          return $ SQL.ColumnDefinition
                     (SQL.UnqualifiedIdentifier name)
                     (SQL.JustType $ subcolumnType subcolumn)
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

