{-# LANGUAGE OverloadedStrings, ExistentialQuantification,
             FlexibleInstances, GADTs, DeriveDataTypeable,
             ScopedTypeVariables #-}
module Schema
  (Schema,
   compile)
  where

import qualified Control.Monad.Error as MTL
import qualified Control.Monad.Identity as MTL
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
import Data.Aeson ((.!=), (.=))
import Data.Dynamic
import Data.Function
import Data.HashMap.Strict (HashMap)
import Data.List
import Data.Map (Map)
import Data.Maybe
import Data.Monoid
import Data.Set (Set)
import Data.Text (Text)

import JSON

import Debug.Trace
import Trace


checkAllowedKeys :: [Text] -> JSON.Object -> JSON.Parser ()
checkAllowedKeys allowedKeys theMap = do
  mapM_ (\(key, _) -> do
            if elem key allowedKeys
              then return ()
              else fail $ "Unknown key " ++ show key ++ ".")
        (HashMap.toList theMap)


instance JSON.FromJSON UUID.UUID where
  parseJSON value@(JSON.String text) =
    case reads $ Text.unpack text of
      [(uuid, "")] -> pure uuid
      _ -> fail $ "Expected UUID but got " ++ (encode value)
  parseJSON value = fail $ "Expected UUID but got " ++ (encode value)
instance JSON.ToJSON UUID.UUID where
  toJSON uuid = JSON.toJSON $ show uuid


instance JSON.FromJSON Timestamp.Timestamp where
  parseJSON value@(JSON.String text) =
    case reads $ Text.unpack text of
      [(timestamp, "")] -> pure $ fromInteger timestamp
      _ -> fail $ "Expected timestamp but got " ++ (encode value)
  parseJSON value = fail $ "Expected timestamp but got " ++ (encode value)
instance JSON.ToJSON Timestamp.Timestamp where
  toJSON timestamp = JSON.toJSON $ show timestamp


data Schema =
  Schema {
      schemaID :: UUID.UUID,
      schemaVersion :: Timestamp.Timestamp,
      schemaTypes :: Map String TypeSpecification,
      schemaTableRoles :: Map String TableRoleSpecification,
      schemaColumnFlags :: Set String,
      schemaColumnRoles :: Map String ColumnRoleSpecification,
      schemaColumnTemplates :: Map String ColumnSpecification,
      schemaEntityFlags :: Set String,
      schemaEntityTemplates :: Map String EntitySpecification,
      schemaEntities :: Map String EntitySpecification
    }
instance JSON.FromJSON Schema where
  parseJSON value@(JSON.Object hashMap) = do
    checkAllowedKeys ["id",
                      "version",
                      "types",
                      "table_roles",
                      "column_flags",
                      "column_roles",
                      "column_templates",
                      "entity_flags",
                      "entity_templates",
                      "entities"]
                     hashMap
    Schema <$> hashMap .: "id"
           <*> hashMap .: "version"
           <*> (hashMap .:? "types" .!= Map.empty)
           <*> (hashMap .:? "table_roles" .!= Map.empty)
           <*> (hashMap .:? "column_flags" .!= Set.empty)
           <*> (hashMap .:? "column_roles" .!= Map.empty)
           <*> (hashMap .:? "column_templates" .!= Map.empty)
           <*> (hashMap .:? "entity_flags" .!= [] >>= return . Set.fromList)
           <*> (hashMap .:? "entity_templates" .!= Map.empty)
           <*> (hashMap .:? "entities" .!= Map.empty)
  parseJSON value = fail $ "Expected schema but got " ++ (encode value)


data TableRoleSpecification =
  TableRoleSpecification {
      tableRoleSpecificationName :: Expression -- NameSpecification
    }
instance JSON.FromJSON TableRoleSpecification where
  parseJSON value@(JSON.Object hashMap) = do
    checkAllowedKeys ["name"]
                     hashMap
    TableRoleSpecification
      <$> pullOutExpressionField hashMap "name"
            (JSON.parseJSON :: JSON.Value -> JSON.Parser NameSpecification)
            False
  parseJSON value = fail $ "Expected table-role but got " ++ (encode value)


data EntitySpecification =
  EntitySpecification {
      entitySpecificationTemplate :: Maybe String,
      entitySpecificationFlags :: Maybe (Map String Bool),
      entitySpecificationTables :: Maybe Expression, -- [String]
      entitySpecificationKey :: Maybe Expression, -- [ColumnSpecification]
      entitySpecificationColumns :: Maybe Expression, -- [ColumnSpecification]
      entitySpecificationRelations
        :: Maybe Expression -- [RelationSpecification]
    }
instance JSON.FromJSON EntitySpecification where
  parseJSON value@(JSON.Object hashMap) = do
    checkAllowedKeys ["template",
                      "flags",
                      "tables",
                      "key",
                      "columns",
                      "relations"]
                     hashMap
    EntitySpecification
      <$> hashMap .:? "template"
      <*> hashMap .:? "flags"
      <*> pullOutMaybeExpressionField hashMap "tables"
            (JSON.parseJSON :: JSON.Value -> JSON.Parser String)
            True
      <*> pullOutMaybeExpressionField hashMap "key"
            (JSON.parseJSON :: JSON.Value -> JSON.Parser ColumnSpecification)
            True
      <*> pullOutMaybeExpressionField hashMap "columns"
            (JSON.parseJSON :: JSON.Value -> JSON.Parser ColumnSpecification)
            True
      <*> pullOutMaybeExpressionField hashMap "relations"
            (JSON.parseJSON :: JSON.Value
                            -> JSON.Parser RelationSpecification)
            True
  parseJSON value = fail $ "Expected entity but got " ++ (encode value)
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
          :: (EntitySpecification -> Maybe Expression)
          -> Maybe Expression
        concatenate accessor = combine accessor (\a' b' ->
          ConcatenateExpression {
              concatenateExpressionItems =
                ListExpression {
                    listExpressionItems = [b', a']
                  }
            })
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
      entitySpecificationFlattenedRelations :: [RelationSpecificationFlattened]
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
      flatten
        :: (Typeable item, Typeable result)
        => String
        -> (EntitySpecification -> Maybe Expression)
        -> (Context -> item -> Compilation result)
        -> Compilation result
      flatten = flattenField flattened emptyContext
      flattenList
        :: (Typeable item, Typeable result)
        => String
        -> (EntitySpecification -> Maybe Expression)
        -> (Context -> item -> Compilation result)
        -> Compilation [result]
      flattenList = flattenListField flattened emptyContext
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
  tables <- flattenList "tables" entitySpecificationTables
              (\context value -> return value)
  if null tables
    then throwError "Entity has no tables."
    else return ()
  key <- flattenList "key" entitySpecificationKey
           (flattenKeyColumnSpecification allColumnFlags columnTemplates)
  columns <- flattenList "columns" entitySpecificationColumns
               (flattenColumnSpecification allColumnFlags columnTemplates)
  relations <- flattenList "relations" entitySpecificationRelations
                 (\context value -> return value)
  return $ EntitySpecificationFlattened {
               entitySpecificationFlattenedFlags = flags,
               entitySpecificationFlattenedTables = tables,
               entitySpecificationFlattenedKey = key,
               entitySpecificationFlattenedColumns = columns,
               entitySpecificationFlattenedRelations = relations
            }


data ColumnSpecification =
  ColumnSpecification {
      columnSpecificationTemplate
        :: Maybe Expression, -- String
      columnSpecificationFlags
        :: Maybe (Map String Expression), -- Bool
      columnSpecificationName
        :: Maybe Expression, -- NameSpecification
      columnSpecificationType
        :: Maybe Expression, -- TypeReferenceSpecification
      columnSpecificationTableRoles
        :: Maybe Expression, -- [String]
      columnSpecificationColumnRole
        :: Maybe Expression, -- String
      columnSpecificationReadOnly
        :: Maybe Expression, -- Bool
      columnSpecificationConcretePathOf
        :: Maybe Expression -- NameSpecification
    }
  deriving (Typeable)
instance JSON.FromJSON ColumnSpecification where
  parseJSON value@(JSON.Object hashMap) = do
    checkAllowedKeys ["template",
                      "flags",
                      "name",
                      "type",
                      "role",
                      "table_roles",
                      "read_only",
                      "concrete_path_of"]
                     hashMap
    flags <- hashMap .:? "flags"
    flags <- case flags of
               Nothing -> return Nothing
               Just flagMap -> do
                 mapM (\(key, flagValue) -> do
                          flagExpression <-
                            parseJSONExpression
                              (JSON.parseJSON :: JSON.Value
                                              -> JSON.Parser Bool)
                              False
                              flagValue
                          return (key, flagExpression))
                      (Map.toList flagMap)
                 >>= return . Just . Map.fromList
    ColumnSpecification
      <$> pullOutMaybeExpressionField hashMap "template"
            (JSON.parseJSON :: JSON.Value -> JSON.Parser String)
            False
      <*> pure flags
      <*> pullOutMaybeExpressionField hashMap "name"
            (JSON.parseJSON :: JSON.Value -> JSON.Parser NameSpecification)
            False
      <*> pullOutMaybeExpressionField hashMap "type"
            (JSON.parseJSON :: JSON.Value
                            -> JSON.Parser TypeReferenceSpecification)
            False
      <*> pullOutMaybeExpressionField hashMap "table_roles"
            (JSON.parseJSON :: JSON.Value -> JSON.Parser String)
            True
      <*> pullOutMaybeExpressionField hashMap "role"
            (JSON.parseJSON :: JSON.Value -> JSON.Parser String)
            False
      <*> pullOutMaybeExpressionField hashMap "read_only"
            (JSON.parseJSON :: JSON.Value -> JSON.Parser Bool)
            False
      <*> pullOutMaybeExpressionField hashMap "concrete_path_of"
            (JSON.parseJSON :: JSON.Value -> JSON.Parser NameSpecification)
            False
  parseJSON value = fail $ "Expected column but got " ++ (encode value)
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
          :: (ColumnSpecification -> Maybe Expression)
          -> Maybe Expression
        concatenate accessor = combine accessor (\a' b' ->
          ConcatenateExpression {
              concatenateExpressionItems =
                ListExpression {
                    listExpressionItems = [b', a']
                  }
            })
        replaceKeys
          :: (ColumnSpecification -> Maybe (Map String a))
          -> Maybe (Map String a)
        replaceKeys accessor = combine accessor (\a b -> Map.union b a)
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
             concatenate columnSpecificationTableRoles,
           columnSpecificationColumnRole =
             replace columnSpecificationColumnRole,
           columnSpecificationReadOnly =
             replace columnSpecificationReadOnly,
           columnSpecificationConcretePathOf =
             replace columnSpecificationConcretePathOf
         }


data KeyColumnSpecificationFlattened =
  KeyColumnSpecificationFlattened {
      keyColumnSpecificationFlattenedName :: NameSpecificationFlattened,
      keyColumnSpecificationFlattenedType
        :: TypeReferenceSpecificationFlattened,
      keyColumnSpecificationFlattenedTableRoles :: Set String,
      keyColumnSpecificationFlattenedColumnRole :: String
    }
  deriving (Typeable)
instance JSON.ToJSON KeyColumnSpecificationFlattened where
  toJSON column =
    JSON.object
      ["name" .= keyColumnSpecificationFlattenedName column,
       "type" .= keyColumnSpecificationFlattenedType column,
       "table_roles" .=
         (Set.toList $ keyColumnSpecificationFlattenedTableRoles column),
       "column_role" .= keyColumnSpecificationFlattenedColumnRole column]


flattenKeyColumnSpecification
  :: Set String
  -> Map String ColumnSpecification
  -> Context
  -> ColumnSpecification
  -> Compilation KeyColumnSpecificationFlattened
flattenKeyColumnSpecification allFlags templates entityContext column = do
  let getRelevantTemplates column = do
        case columnSpecificationTemplate column of
          Nothing -> return [column]
          Just templateNameExpression -> do
            templateName <- evaluate entityContext templateNameExpression
                                     (undefined :: String)
            case Map.lookup templateName templates of
              Nothing -> return [column]
              Just template -> do
                rest <- getRelevantTemplates template
                return $ column : rest
  relevantTemplates <- getRelevantTemplates column >>= return . reverse
  let flattened = mconcat relevantTemplates
      flatten
        :: (Typeable item, Typeable result)
        => String
        -> (ColumnSpecification -> Maybe Expression)
        -> (Context -> item -> Compilation result)
        -> Compilation result
      flatten = flattenField flattened entityContext
      flattenList
        :: (Typeable item, Typeable result)
        => String
        -> (ColumnSpecification -> Maybe Expression)
        -> (Context -> item -> Compilation result)
        -> Compilation [result]
      flattenList = flattenListField flattened entityContext
  name <- flatten "name" columnSpecificationName flattenNameSpecification
  type' <- flatten "type" columnSpecificationType
                   flattenTypeReferenceSpecification
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
  tableRoles <- flattenList "table_roles" columnSpecificationTableRoles
                            (\context value -> return value)
                >>= return . Set.fromList
  if Set.null tableRoles
    then throwError "Column has no table roles."
    else return ()
  columnRole <- flatten "column_role" columnSpecificationColumnRole
                        (\context value -> return value)
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


data ColumnSpecificationFlattened =
  ColumnSpecificationFlattened {
      columnSpecificationFlattenedName :: NameSpecificationFlattened,
      columnSpecificationFlattenedType :: TypeReferenceSpecificationFlattened,
      columnSpecificationFlattenedTableRoles :: Set String,
      columnSpecificationFlattenedColumnRole :: String,
      columnSpecificationFlattenedReadOnly :: Bool,
      columnSpecificationFlattenedConcretePathOf
        :: Maybe NameSpecificationFlattened
    }
  deriving (Typeable)
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


flattenColumnSpecification
  :: Set String
  -> Map String ColumnSpecification
  -> Context
  -> ColumnSpecification
  -> Compilation ColumnSpecificationFlattened
flattenColumnSpecification allFlags templates entityContext column = do
  let getRelevantTemplates column = do
        case columnSpecificationTemplate column of
          Nothing -> return [column]
          Just templateNameExpression -> do
            templateName <- evaluate entityContext templateNameExpression
                                     (undefined :: String)
            case Map.lookup templateName templates of
              Nothing -> return [column]
              Just template -> do
                rest <- getRelevantTemplates template
                return $ column : rest
  relevantTemplates <- getRelevantTemplates column >>= return . reverse
  let flattened = mconcat relevantTemplates
      flatten
        :: (Typeable item, Typeable result)
        => String
        -> (ColumnSpecification -> Maybe Expression)
        -> (Context -> item -> Compilation result)
        -> Compilation result
      flatten = flattenField flattened entityContext
      flattenList
        :: (Typeable item, Typeable result)
        => String
        -> (ColumnSpecification -> Maybe Expression)
        -> (Context -> item -> Compilation result)
        -> Compilation [result]
      flattenList = flattenListField flattened entityContext
  name <- flatten "name" columnSpecificationName flattenNameSpecification
  type' <- flatten "type" columnSpecificationType
                   flattenTypeReferenceSpecification
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
  tableRoles <- flattenList "table_roles" columnSpecificationTableRoles
                            (\context value -> return value)
                >>= return . Set.fromList
  if Set.null tableRoles
    then throwError "Column has no table roles."
    else return ()
  columnRole <- flatten "column_role" columnSpecificationColumnRole
                        (\context value -> return value)
  readOnly <- flatten "read_only" columnSpecificationReadOnly
                      (\context value -> return value)
  concretePathOf <-
    case columnSpecificationConcretePathOf flattened of
      Nothing -> return Nothing
      Just value -> do
        value <- evaluate entityContext value (undefined :: NameSpecification)
        value <- flattenNameSpecification entityContext value
        return $ Just value
  return $ ColumnSpecificationFlattened {
               columnSpecificationFlattenedName = name,
               columnSpecificationFlattenedType = type',
               columnSpecificationFlattenedTableRoles = tableRoles,
               columnSpecificationFlattenedColumnRole = columnRole,
               columnSpecificationFlattenedReadOnly = readOnly,
               columnSpecificationFlattenedConcretePathOf = concretePathOf
             }


data ColumnRoleSpecification =
  ColumnRoleSpecification {
      columnRoleSpecificationPriority :: Int
    }
instance JSON.FromJSON ColumnRoleSpecification where
  parseJSON value@(JSON.Object hashMap) = do
    checkAllowedKeys ["priority"]
                     hashMap
    ColumnRoleSpecification <$> hashMap .: "priority"
  parseJSON value = fail $ "Expected column-role but got " ++ (encode value)
instance JSON.ToJSON ColumnRoleSpecification where
  toJSON columnRole =
    JSON.object
     ["priority" .= (JSON.toJSON
       $ columnRoleSpecificationPriority columnRole)]


data RelationSpecification =
  RelationSpecification {
      relationSpecificationEntity :: Expression, -- String
      relationSpecificationPurpose :: Maybe Expression, -- String
      relationSpecificationRequired :: Expression, -- Bool
      relationSpecificationUnique :: Expression, -- Bool
      relationSpecificationKey :: Maybe Expression -- [NameSpecification]
    }
  deriving (Typeable)
instance JSON.FromJSON RelationSpecification where
  parseJSON value@(JSON.Object hashMap) = do
    checkAllowedKeys ["entity",
                      "purpose",
                      "required",
                      "unique",
                      "key"]
                     hashMap
    RelationSpecification
      <$> pullOutExpressionField hashMap "entity"
            (JSON.parseJSON :: JSON.Value -> JSON.Parser String)
            False
      <*> pullOutMaybeExpressionField hashMap "purpose"
            (JSON.parseJSON :: JSON.Value -> JSON.Parser String)
            False
      <*> pullOutExpressionField hashMap "required"
            (JSON.parseJSON :: JSON.Value -> JSON.Parser Bool)
            False
      <*> pullOutExpressionField hashMap "unique"
            (JSON.parseJSON :: JSON.Value -> JSON.Parser Bool)
            False
      <*> pullOutMaybeExpressionField hashMap "key"
            (JSON.parseJSON :: JSON.Value -> JSON.Parser NameSpecification)
            True


data RelationSpecificationFlattened =
  RelationSpecificationFlattened {
      relationSpecificationFlattenedEntity :: String,
      relationSpecificationFlattenedPurpose :: Maybe String,
      relationSpecificationFlattenedRequired :: Bool,
      relationSpecificationFlattenedUnique :: Bool,
      relationSpecificationFlattenedKey :: Maybe [NameSpecificationFlattened]
    }
  deriving (Typeable)
instance JSON.ToJSON RelationSpecificationFlattened where
  toJSON relation =
    JSON.object
      ["entity" .= JSON.toJSON (relationSpecificationFlattenedEntity relation),
       "purpose" .= JSON.toJSON
         (relationSpecificationFlattenedPurpose relation),
       "required" .= JSON.toJSON
         (relationSpecificationFlattenedRequired relation),
       "unique" .= JSON.toJSON (relationSpecificationFlattenedUnique relation),
       "key" .= JSON.toJSON (relationSpecificationFlattenedKey relation)]


flattenRelationSpecification
  :: Context
  -> RelationSpecification
  -> Compilation RelationSpecificationFlattened
flattenRelationSpecification entityContext relation = do
  entity <- evaluate entityContext (relationSpecificationEntity relation)
                     (undefined :: String)
  purpose <-
    case relationSpecificationPurpose relation of
      Nothing -> return Nothing
      Just purpose -> evaluate entityContext purpose (undefined :: String)
                      >>= return . Just
  required <- evaluate entityContext (relationSpecificationRequired relation)
                       (undefined :: Bool)
  unique <- evaluate entityContext (relationSpecificationUnique relation)
                     (undefined :: Bool)
  key <-
    case relationSpecificationKey relation of
      Nothing -> return Nothing
      Just key -> evaluate entityContext key (undefined :: NameSpecification)
                  >>= mapM (flattenNameSpecification entityContext)
                  >>= return . Just
  return $ RelationSpecificationFlattened {
               relationSpecificationFlattenedEntity = entity,
               relationSpecificationFlattenedPurpose = purpose,
               relationSpecificationFlattenedRequired = required,
               relationSpecificationFlattenedUnique = unique,
               relationSpecificationFlattenedKey = key
             }


data NameSpecification =
  NameSpecification Expression -- [NameSpecificationPart]
  deriving (Typeable)
instance JSON.FromJSON NameSpecification where
  parseJSON (JSON.String text) =
    pure $ NameSpecification $
      ConstantExpression {
          constantExpressionValue =
            toDyn $ [LiteralNameSpecificationPart $ Text.unpack text]
        }
  parseJSON items@(JSON.Array _) =
    NameSpecification <$>
      (parseJSONExpression
        (JSON.parseJSON :: JSON.Value -> JSON.Parser NameSpecificationPart)
        True
        items)
  parseJSON value = fail $ "Expected name but got " ++ (encode value)


data NameSpecificationFlattened =
  NameSpecificationFlattened [NameSpecificationPart]
  deriving (Typeable)
instance JSON.ToJSON NameSpecificationFlattened where
  toJSON (NameSpecificationFlattened parts) = JSON.toJSON parts


data NameSpecificationPart
  = LiteralNameSpecificationPart String
  | VariableNameSpecificationPart String
  deriving (Typeable)
instance JSON.FromJSON NameSpecificationPart where
  parseJSON (JSON.String text) =
    pure $ LiteralNameSpecificationPart $ Text.unpack text
  parseJSON value@(JSON.Object hashMap) = do
    type' <- hashMap .: "type"
    return (type' :: String)
    case type' of
      "constant" -> do
        checkAllowedKeys ["type",
                          "value"]
                         hashMap
        LiteralNameSpecificationPart <$> hashMap .: "value"
      "variable" -> do
        checkAllowedKeys ["type",
                          "name"]
                         hashMap
        VariableNameSpecificationPart <$> hashMap .: "name"
      _ -> fail $ "Expected name part but got " ++ (encode value)
  parseJSON value = fail $ "Expected name part but got " ++ (encode value)
instance JSON.ToJSON NameSpecificationPart where
  toJSON (LiteralNameSpecificationPart value) =
    JSON.object
      ["type" .= JSON.toJSON ("constant" :: Text),
       "value" .= JSON.toJSON value]
  toJSON (VariableNameSpecificationPart name) =
    JSON.object
      ["type" .= JSON.toJSON ("variable" :: Text),
       "name" .= JSON.toJSON name]


data TypeReferenceSpecification =
  TypeReferenceSpecification
    Expression -- String
    Expression -- [TypeReferenceSpecification]
  deriving (Typeable)
instance JSON.FromJSON TypeReferenceSpecification where
  parseJSON value@(JSON.Array _) = do
    items <- JSON.parseJSON value
    case items of
      (constructor : parameters) -> do
        constructor <-
          parseJSONExpression
            (JSON.parseJSON :: JSON.Value -> JSON.Parser String)
            False
            constructor
        parameters <- mapM
          (parseJSONExpression
            (JSON.parseJSON
              :: JSON.Value -> JSON.Parser TypeReferenceSpecification)
            False)
          parameters
        let parameterList = ListExpression {
                             listExpressionItems = parameters
                           }
        return $ TypeReferenceSpecification constructor parameterList
      _ -> do
        item <-
          (parseJSONExpression
            (JSON.parseJSON :: JSON.Value -> JSON.Parser String)
            False
            value)
        return $ TypeReferenceSpecification item
                   (ConstantExpression {
                        constantExpressionValue =
                          toDyn ([] :: [TypeReferenceSpecification])
                      })
  parseJSON value = do
    item <-
      (parseJSONExpression
        (JSON.parseJSON :: JSON.Value -> JSON.Parser String)
        False
        value)
    return $ TypeReferenceSpecification item
               (ConstantExpression {
                    constantExpressionValue =
                      toDyn ([] :: [TypeReferenceSpecification])
                  })


data TypeReferenceSpecificationFlattened =
  TypeReferenceSpecificationFlattened
    String [TypeReferenceSpecificationFlattened]
  deriving (Typeable)
instance JSON.ToJSON TypeReferenceSpecificationFlattened where
  toJSON (TypeReferenceSpecificationFlattened constructor parameters) =
    let parameters' =
          fromJust $ JSON.parseMaybe JSON.parseJSON (JSON.toJSON parameters)
    in JSON.toJSON (constructor : parameters')


flattenTypeReferenceSpecification
  :: Context
  -> TypeReferenceSpecification
  -> Compilation TypeReferenceSpecificationFlattened
flattenTypeReferenceSpecification
    context (TypeReferenceSpecification constructor parameters) = do
  constructor <- evaluate context constructor (undefined :: String)
  parameters <- evaluate context parameters
                         (undefined :: TypeReferenceSpecification)
  parameters <- mapM (flattenTypeReferenceSpecification context) parameters
  return $ TypeReferenceSpecificationFlattened constructor parameters


data TypeSpecification =
  TypeSpecification {
      typeSpecificationParameters :: Maybe Expression, -- [String]
      typeSpecificationSubcolumns
        :: Maybe Expression -- [SubcolumnSpecification]
    }
instance JSON.FromJSON TypeSpecification where
  parseJSON value@(JSON.Object hashMap) = do
    checkAllowedKeys ["parameters",
                      "subcolumns"]
                     hashMap
    TypeSpecification
      <$> pullOutMaybeExpressionField hashMap "parameters"
            (JSON.parseJSON :: JSON.Value -> JSON.Parser String)
            True
      <*> pullOutMaybeExpressionField hashMap "subcolumns"
            (JSON.parseJSON :: JSON.Value
                            -> JSON.Parser SubcolumnSpecification)
            True
  parseJSON value = fail $ "Expected type but got " ++ (encode value)


data TypeSpecificationFlattened =
  TypeSpecificationFlattened {
      typeSpecificationFlattenedParameters :: [String],
      typeSpecificationFlattenedSubcolumns :: [SubcolumnSpecificationFlattened]
    }
instance JSON.ToJSON TypeSpecificationFlattened where
  toJSON specification =
    JSON.object
      ["parameters" .= (JSON.toJSON $
         typeSpecificationFlattenedParameters specification),
       "subcolumns" .= (JSON.toJSON $
         typeSpecificationFlattenedSubcolumns specification)]


flattenTypeSpecification
  :: Context
  -> TypeSpecification
  -> Compilation TypeSpecificationFlattened
flattenTypeSpecification context specification = do
  let flatten
        :: (Typeable item, Typeable result)
        => String
        -> (TypeSpecification -> Maybe Expression)
        -> (Context -> item -> Compilation result)
        -> Compilation result
      flatten = flattenField specification context
      flattenList
        :: (Typeable item, Typeable result)
        => String
        -> (TypeSpecification -> Maybe Expression)
        -> (Context -> item -> Compilation result)
        -> Compilation [result]
      flattenList = flattenListField specification context
  parameters <- flatten "parameters" typeSpecificationParameters
                         (\context value -> return value)
  subcolumns <- flattenList "subcolumns" typeSpecificationSubcolumns
                            flattenSubcolumnSpecification
  return $ TypeSpecificationFlattened {
               typeSpecificationFlattenedParameters = parameters,
               typeSpecificationFlattenedSubcolumns = subcolumns
             }


data SubcolumnSpecification =
  SubcolumnSpecification {
      subcolumnSpecificationName :: Maybe Expression, -- NameSpecification
      subcolumnSpecificationType :: Maybe Expression -- PrimitiveType
    }
  deriving (Typeable)
instance JSON.FromJSON SubcolumnSpecification where
  parseJSON value@(JSON.Object hashMap) = do
    checkAllowedKeys ["name",
                      "type"]
                     hashMap
    SubcolumnSpecification
      <$> pullOutMaybeExpressionField hashMap "name"
            (JSON.parseJSON :: JSON.Value -> JSON.Parser NameSpecification)
            False
      <*> pullOutMaybeExpressionField hashMap "type"
            (JSON.parseJSON :: JSON.Value -> JSON.Parser PrimitiveType)
            False
  parseJSON value = fail $ "Expected subcolumn but got " ++ (encode value)


data SubcolumnSpecificationFlattened =
  SubcolumnSpecificationFlattened {
      subcolumnSpecificationFlattenedName :: NameSpecificationFlattened,
      subcolumnSpecificationFlattenedType :: PrimitiveType
    }
  deriving (Typeable)
instance JSON.ToJSON SubcolumnSpecificationFlattened where
  toJSON specification =
    JSON.object
      ["name" .= (JSON.toJSON $
         subcolumnSpecificationFlattenedName specification),
       "type" .= (JSON.toJSON $
         subcolumnSpecificationFlattenedType specification)]


flattenSubcolumnSpecification
  :: Context
  -> SubcolumnSpecification
  -> Compilation SubcolumnSpecificationFlattened
flattenSubcolumnSpecification context specification = do
  let flatten
        :: (Typeable item, Typeable result)
        => String
        -> (SubcolumnSpecification -> Maybe Expression)
        -> (Context -> item -> Compilation result)
        -> Compilation result
      flatten = flattenField specification context
  name <- flatten "name" subcolumnSpecificationName flattenNameSpecification
  type' <- flatten "type" subcolumnSpecificationType
                   (\context value -> return value)
  return $ SubcolumnSpecificationFlattened {
               subcolumnSpecificationFlattenedName = name,
               subcolumnSpecificationFlattenedType = type'
             }


data Context =
  Context {
      contextFlags :: Map String Bool,
      contextVariables :: Map String Dynamic
    }


emptyContext :: Context
emptyContext =
  Context {
      contextFlags = Map.empty,
      contextVariables = Map.empty
    }


data Expression
  = VariableExpression {
        variableExpressionName :: String
      }
  | ConstantExpression {
        constantExpressionValue :: Dynamic
      }
  | ListExpression {
        listExpressionItems :: [Expression] -- Anything
      }
  | ConditionalExpression {
        conditionalExpressionItems :: [(Condition, Expression)], -- Anything
        conditionalExpressionDefaultItem :: Expression -- Anything
      }
  | SubcolumnsExpression {
        subcolumnsExpressionType :: Expression -- Type
      }
  | ConcatenateExpression {
        concatenateExpressionItems :: Expression -- List of lists
      }
  deriving (Typeable)


parseJSONExpression
  :: (Typeable content)
  => (JSON.Value -> JSON.Parser content)
  -> Bool
  -> JSON.Value
  -> JSON.Parser Expression
parseJSONExpression underlyingParser isList value@(JSON.Array _) = do
  items <- JSON.parseJSON value
  case items of
    (JSON.String variable' : JSON.String name : [])
      | variable' == "variable" -> do
        return $ VariableExpression {
                     variableExpressionName = Text.unpack name
                   }
    (JSON.String case' : conditions)
      | case' == "case" -> do
        default' <- JSON.parseJSON (last conditions)
        default' <-
          case default' of
            (JSON.String default' : subvalue : [])
              | default' == "default" ->
                  parseJSONExpression underlyingParser isList subvalue
            _ -> fail $ "No default condition in case expression."
        conditions <- mapM (\subvalue -> do
                              (condition, expression) <-
                                JSON.parseJSON subvalue
                              expression <-
                                parseJSONExpression underlyingParser isList
                                                    expression
                              return (condition, expression))
                           (init conditions)
        return $ ConditionalExpression {
                     conditionalExpressionItems = conditions,
                     conditionalExpressionDefaultItem = default'
                   }
    ((JSON.String subcolumns) : type' : [])
      | subcolumns == "subcolumns" -> do
        type' <- parseJSONExpression
          (JSON.parseJSON
            :: JSON.Value
            -> JSON.Parser TypeReferenceSpecification)
          False
          type'
        return $ SubcolumnsExpression {
                     subcolumnsExpressionType = type'
                   }
    ((JSON.String concatenate) : items : [])
      | concatenate == "concatenate" -> do
        items <- parseJSONExpression underlyingParser isList items
        return $ ConcatenateExpression {
                     concatenateExpressionItems = items
                   }
    _ -> do
      if isList
        then do
          let helper toplevel item = do
                case item of
                  value@(JSON.Array _) -> do
                    items <- JSON.parseJSON value
                    case items of
                      (JSON.String "when" : condition : items) -> do
                        condition <- JSON.parseJSON condition
                        parsedItems <- mapM (helper False) items
                        return $ ConditionalExpression {
                                     conditionalExpressionItems =
                                       [(condition,
                                         ConcatenateExpression {
                                             concatenateExpressionItems =
                                               ListExpression {
                                                   listExpressionItems =
                                                     parsedItems
                                                }
                                           })],
                                     conditionalExpressionDefaultItem =
                                       ListExpression {
                                           listExpressionItems = []
                                         }
                                   }
                      _ -> do
                        items <- mapM (helper False) items
                        return $ ListExpression {
                                     listExpressionItems = items
                                   }
                  value ->
                    if toplevel
                      then fail $ "Expected list but got " ++ (encode value)
                      else do
                        item <- underlyingParser value
                        return $ ConstantExpression {
                                     constantExpressionValue = toDyn item
                                   }
          items <- helper True value
          return $ ConcatenateExpression {
                       concatenateExpressionItems =
                         ListExpression {
                             listExpressionItems = [items]
                           }
                     }
        else do
          item <- underlyingParser value
          return $ ConstantExpression {
                       constantExpressionValue = toDyn item
                     }
parseJSONExpression underlyingParser isList value = do
  item <- underlyingParser value
  return $ ConstantExpression {
               constantExpressionValue = toDyn item
             }


pullOutExpressionField
  :: (Typeable content)
  => JSON.Object
  -> Text
  -> (JSON.Value -> JSON.Parser content)
  -> Bool
  -> JSON.Parser Expression
pullOutExpressionField value field underlyingParser isList = do
  wrapErrors ("Parsing " ++ Text.unpack field) $ do
    subvalue <- value .: field
    parseJSONExpression underlyingParser isList subvalue


pullOutMaybeExpressionField
  :: (Typeable content)
  => JSON.Object
  -> Text
  -> (JSON.Value -> JSON.Parser content)
  -> Bool
  -> JSON.Parser (Maybe Expression)
pullOutMaybeExpressionField value field underlyingParser isList = do
  wrapErrors ("Parsing " ++ Text.unpack field) $ do
    maybeSubvalue <- value .:? field
    case maybeSubvalue of
      Nothing -> return Nothing
      Just subvalue -> parseJSONExpression underlyingParser isList subvalue
        >>= return . Just


evaluate
  :: forall content item . (Typeable content, Typeable item)
  => Context
  -> Expression
  -> item
  -> Compilation content
evaluate context expression@(VariableExpression { }) _ = do
  let name = variableExpressionName expression
  case Map.lookup name (contextVariables context) of
    Nothing -> throwError $ "Reference to undefined variable \"" ++ name
                            ++ "\"."
    Just dynamicValue -> do
      case fromDynamic dynamicValue of
        Nothing -> throwError $ "Variable \"" ++ name
                                ++ "\" not of expected type."
        Just value -> return value
evaluate context expression@(ConstantExpression { }) _ = do
  let dynamicResult = constantExpressionValue expression
  case fromDynamic dynamicResult of
    Nothing -> throwError $ "Expression of unexpected type."
    Just result -> return result
evaluate context expression@(ListExpression { }) witness = do
  result <-
    foldM (\soFar item -> do
             item <- evaluate context item witness
             return $ soFar ++ [item])
          ([] :: [item])
          (listExpressionItems expression)
  let dynamicResult = toDyn result
  case fromDynamic dynamicResult of
    Nothing -> throwError $ "List expression unexpected."
    Just result -> return result
evaluate context expression@(ConditionalExpression { }) witness = do
  maybeResult <-
    foldM (\maybeResult (condition, content) -> do
             case maybeResult of
               Just _ -> return maybeResult
               Nothing -> do
                 applicable <- testCondition context condition
                 if applicable
                   then return $ Just content
                   else return Nothing)
          Nothing
          (conditionalExpressionItems expression)
  let result =
        fromMaybe (conditionalExpressionDefaultItem expression) maybeResult
  result <- evaluate context result witness
  let dynamicResult = toDyn (result :: content)
  case fromDynamic dynamicResult of
    Nothing -> throwError $ "Expression of unexpected type."
    Just result -> return result
evaluate context expression@(SubcolumnsExpression { }) _ = do
  type' <- evaluate context (subcolumnsExpressionType expression) 
                    (undefined :: Type)
  let dynamicResult = toDyn $ typeSubcolumns type'
  case fromDynamic dynamicResult of
    Nothing -> throwError $ "Subcolumn-list expression unexpected."
    Just result -> return result
evaluate context expression@(ConcatenateExpression { }) witness = do
  case expression of
    ConcatenateExpression {
        concatenateExpressionItems = itemsExpression
      } -> do
        items <- evaluate context itemsExpression witness
        let dynamicResult = toDyn $ (concat items :: [content])
        case fromDynamic dynamicResult of
          Nothing -> throwError $ "List expression unexpected."
          Just result -> return result


data Condition
  = Condition {
        conditionFlags :: Map String Bool
      }
instance JSON.FromJSON Condition where
  parseJSON (JSON.String text)
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
      _ -> fail $ "Expected condition but got " ++ (encode values)
  parseJSON items@(JSON.Object _) = do
    JSON.parseJSON items >>= return . Condition . Map.fromList
  parseJSON value = fail $ "Expected condition but got " ++ (encode value)
instance JSON.ToJSON Condition where
  toJSON condition@(Condition { }) = do
    JSON.toJSON $ conditionFlags condition


testCondition
  :: Context
  -> Condition
  -> Compilation Bool
testCondition context condition = do
  foldM (\result (flag, expected) -> do
           if not result
             then return False
             else do
               case Map.lookup flag (contextFlags context) of
                 Nothing -> throwError $ "Flag \"" ++ flag
                                         ++ "\" referenced but not defined."
                 Just actual -> return $ expected == actual)
        True
        (Map.toList $ conditionFlags condition)


data Type =
  Type {
      typeSubcolumns :: [Subcolumn]
    }
  deriving (Typeable)
instance JSON.ToJSON Type where
  toJSON type' =
    JSON.object
      ["subcolumns" .= (JSON.toJSON $ typeSubcolumns type')]


data TableRole =
  TableRole {
      tableRoleName :: String,
      tableRoleTableName :: NameSpecificationFlattened
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
      columnType :: TypeReferenceSpecificationFlattened,
      columnReadOnly :: Bool,
      columnConcretePathOf :: Maybe String
    }
instance JSON.ToJSON Column where
  toJSON column =
    JSON.object
      ["name" .= JSON.toJSON (columnName column),
       "type" .= JSON.toJSON (columnType column),
       "read_only" .= JSON.toJSON (columnReadOnly column),
       "concrete_path_of" .= JSON.toJSON (columnConcretePathOf column)]


data PreEntity =
  PreEntity {
      preEntityName :: String,
      preEntityFlags :: Map String Bool,
      preEntityTableRoles :: Map String TableRole,
      preEntityColumnRoles :: Map String ColumnRole,
      preEntityKeyColumns :: [PreColumn],
      preEntityDataColumns :: [ColumnSpecificationFlattened],
      preEntityRelations :: [RelationSpecificationFlattened]
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
      preColumnName :: NameSpecificationFlattened,
      preColumnType :: TypeReferenceSpecificationFlattened,
      preColumnReadOnly :: Bool,
      preColumnTableRoles :: Set String,
      preColumnRole :: ColumnRole,
      preColumnConcretePathOf :: Maybe NameSpecificationFlattened
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
      subcolumnName :: NameSpecificationFlattened,
      subcolumnType :: PrimitiveType
    }
  deriving (Typeable)
instance JSON.ToJSON Subcolumn where
  toJSON subcolumn =
    JSON.object
      ["name" .= JSON.toJSON (subcolumnName subcolumn),
       "type" .= JSON.toJSON (subcolumnType subcolumn)]


data PrimitiveType
  = IntegerPrimitiveType
  | NumericPrimitiveType
  | BlobPrimitiveType
  | TextPrimitiveType
  deriving (Typeable)
instance JSON.FromJSON PrimitiveType where
  parseJSON value@(JSON.String text)
    | text == "integer" = pure IntegerPrimitiveType
    | text == "numeric" = pure NumericPrimitiveType
    | text == "blob" = pure BlobPrimitiveType
    | text == "text" = pure TextPrimitiveType
    | otherwise = fail $ "Expected primitive type but got " ++ (encode value)
  parseJSON value = fail $ "Expected primitive type but got " ++ (encode value)
instance JSON.ToJSON PrimitiveType where
  toJSON IntegerPrimitiveType = JSON.String "integer"
  toJSON NumericPrimitiveType = JSON.String "numeric"
  toJSON BlobPrimitiveType = JSON.String "blob"
  toJSON TextPrimitiveType = JSON.String "text"


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
  types <- mapM (\(name, type') -> do
                    type' <- flattenTypeSpecification emptyContext type'
                    return (name, type'))
                (Map.toList $ schemaTypes schema)
           >>= return . Map.fromList
  tableRoles <- compileTableRoles emptyContext (schemaTableRoles schema)
  columnRoles <- compileColumnRoles (schemaColumnRoles schema)
  entities <- compileEntities tableRoles
                              columnRoles
                              (schemaEntityFlags schema)
                              (schemaEntityTemplates schema)
                              (schemaColumnFlags schema)
                              (schemaColumnTemplates schema)
                              (schemaEntities schema)
  tables <- mapM (compileTable types)
                 $ concatMap (\entity -> Map.elems $ entityTables entity)
                             (Map.elems entities)
  return $ D.Schema {
      D.schemaID = schemaID schema,
      D.schemaVersion = schemaVersion schema,
      D.schemaTables = tables
    }


compileTableRoles
  :: Context
  -> Map String TableRoleSpecification
  -> Compilation (Map String TableRole)
compileTableRoles context tableRoles = do
  mapM (\(name, tableRole) -> do
           tableRole <- compileTableRole context name tableRole
           return (name, tableRole))
       (Map.toList tableRoles)
  >>= return . Map.fromList


compileTableRole
  :: Context
  -> String
  -> TableRoleSpecification
  -> Compilation TableRole
compileTableRole context name tableRole = do
  tableName <-
    flattenField tableRole context
                 "name"
                 (Just . tableRoleSpecificationName)
                 flattenNameSpecification
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
                concretePathOf <-
                  case preColumnConcretePathOf column of
                    Nothing -> return Nothing
                    Just concretePathOf -> do
                      substituteNameSpecification
                        substitutionVariables
                        concretePathOf
                      >>= finalizeNameSpecification
                      >>= return . Just
                return (name, Column {
                                 columnName = name,
                                 columnType = preColumnType column,
                                 columnReadOnly = preColumnReadOnly column,
                                 columnConcretePathOf = concretePathOf
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


flattenNameSpecification
  :: Context
  -> NameSpecification
  -> Compilation NameSpecificationFlattened
flattenNameSpecification context (NameSpecification parts) = do
  evaluate context parts (undefined :: NameSpecificationPart)
  >>= return . NameSpecificationFlattened


finalizeNameSpecification
  :: NameSpecificationFlattened
  -> Compilation String
finalizeNameSpecification (NameSpecificationFlattened parts) = do
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
  -> NameSpecificationFlattened
  -> Compilation NameSpecificationFlattened
substituteNameSpecification bindings (NameSpecificationFlattened parts) = do
  parts <- mapM (\part -> do
                   case part of
                     VariableNameSpecificationPart variable -> do
                       case Map.lookup variable bindings of
                         Nothing -> return part
                         Just value ->
                           return $ LiteralNameSpecificationPart value
                     _ -> return part)
                parts
  return $ NameSpecificationFlattened parts


flattenField
  :: forall object field flattened . (Typeable field, Typeable flattened)
  => object
  -> Context
  -> String
  -> (object -> Maybe Expression)
  -> (Context -> field -> Compilation flattened)
  -> Compilation flattened
flattenField object context field accessor flattener = do
  case accessor object of
    Nothing -> throwError $ "Field \"" ++ field ++ "\" undefined."
    Just value -> do
      value <- evaluate context value (undefined :: flattened)
      flattener context value


flattenListField
  :: forall object field flattened . (Typeable field, Typeable flattened)
  => object
  -> Context
  -> String
  -> (object -> Maybe Expression)
  -> (Context -> field -> Compilation flattened)
  -> Compilation [flattened]
flattenListField object context field accessor flattener = do
  case accessor object of
    Nothing -> throwError $ "Field \"" ++ field ++ "\" undefined."
    Just value -> do
      values <- evaluate context value (undefined :: flattened)
      mapM (flattener context) values


compileTable
  :: Map String TypeSpecificationFlattened
  -> Table
  -> Compilation SQL.CreateTable
compileTable allTypes table =
  tagErrors ("Compiling table \"" ++ (tableName table) ++ "\"") $ do
    columns <- mapM (compileColumn allTypes) (Map.elems $ tableColumns table)
               >>= return . concat
    case SQL.mkOneOrMore columns of
      Nothing -> throwError "No columns."
      Just columns ->
        return $ SQL.CreateTable SQL.NoTemporary
                   SQL.NoIfNotExists
                     (SQL.SinglyQualifiedIdentifier Nothing $ tableName table)
                     $ SQL.ColumnsAndConstraints columns []


compileColumn
  :: Map String TypeSpecificationFlattened
  -> Column
  -> Compilation [SQL.ColumnDefinition]
compileColumn allTypes column = do
  type' <- compileTypeReference allTypes (columnType column)
  mapM (\subcolumn -> do
          name <- substituteNameSpecification
                    (Map.fromList [("column", columnName column)])
                    (subcolumnName subcolumn)
                  >>= finalizeNameSpecification
          type' <- compilePrimitiveType $ subcolumnType subcolumn
          return $ SQL.ColumnDefinition
                     (SQL.UnqualifiedIdentifier name)
                     (SQL.JustType type')
                     [])
       (typeSubcolumns type')


compileTypeReference
  :: Map String TypeSpecificationFlattened
  -> TypeReferenceSpecificationFlattened
  -> Compilation Type
compileTypeReference types reference = do
  throwError $ "Unknown type \"" ++ "..." ++ "\"."


compilePrimitiveType :: PrimitiveType -> Compilation SQL.Type
compilePrimitiveType IntegerPrimitiveType =
  return $ SQL.Type SQL.TypeAffinityNone
                    (SQL.TypeName $ fromJust $ SQL.mkOneOrMore
                      [SQL.UnqualifiedIdentifier "integer"])
                    SQL.NoTypeSize
compilePrimitiveType NumericPrimitiveType =
  return $ SQL.Type SQL.TypeAffinityNone
                    (SQL.TypeName $ fromJust $ SQL.mkOneOrMore
                      [SQL.UnqualifiedIdentifier "numeric"])
                    SQL.NoTypeSize
compilePrimitiveType BlobPrimitiveType =
  return $ SQL.Type SQL.TypeAffinityNone
                    (SQL.TypeName $ fromJust $ SQL.mkOneOrMore
                      [SQL.UnqualifiedIdentifier "blob"])
                    SQL.NoTypeSize
compilePrimitiveType TextPrimitiveType =
  return $ SQL.Type SQL.TypeAffinityNone
                    (SQL.TypeName $ fromJust $ SQL.mkOneOrMore
                      [SQL.UnqualifiedIdentifier "text"])
                    SQL.NoTypeSize
