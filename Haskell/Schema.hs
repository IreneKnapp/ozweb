{-# LANGUAGE OverloadedStrings #-}
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

import Data.Aeson ((.:), (.:?), (.!=), (.=))
import qualified Data.Aeson as JSON
import qualified Data.Map as Map
import qualified Data.Text as Text
import qualified Data.UUID as UUID
import qualified Database as D
import qualified Timestamp as Timestamp

import Control.Applicative
import Control.Monad
import Data.List
import Data.Map (Map)
import Data.Maybe
import Data.Monoid
import Data.Text (Text)

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


data TypeSpecification = TypeSpecification String [String]
instance JSON.FromJSON TypeSpecification where
  parseJSON (JSON.String text) =
    pure $ TypeSpecification (Text.unpack text) []
  parseJSON items@(JSON.Array _) = do
    items <- JSON.parseJSON items
    case items of
      (constructor : parameters) ->
        return $ TypeSpecification constructor parameters
      _ -> mzero
  parseJSON _ = mzero
instance JSON.ToJSON TypeSpecification where
  toJSON (TypeSpecification constructor parameters) =
    JSON.toJSON (constructor : parameters)


data Entity =
  Entity {
      entityName :: String
    }


flattenEntitySpecification
  :: Map String EntitySpecification
  -> EntitySpecification
  -> EntitySpecificationFlattened
flattenEntitySpecification templates entity =
  let relevantTemplates =
        unfoldr (\maybeEntity ->
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


computeEntities
  :: Map String EntitySpecification
  -> Map String EntitySpecification
  -> Map String Entity
computeEntities templates entities =
  traceJSON (Map.map (flattenEntitySpecification templates) entities)
  $ Map.mapWithKey
    (\name entity ->
       let flattened = flattenEntitySpecification templates entity
       in Entity {
              entityName = name
            })
    entities


compile :: Schema -> D.Schema
compile schema =
  let entities = computeEntities (schemaEntityTemplates schema)
                                 (schemaEntities schema)
      tables = []
  in seq entities D.Schema {
         D.schemaID = schemaID schema,
         D.schemaVersion = schemaVersion schema,
         D.schemaTables = tables
       }
