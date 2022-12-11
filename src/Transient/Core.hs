{-# LANGUAGE AllowAmbiguousTypes    #-}
{-# LANGUAGE DataKinds              #-}
{-# LANGUAGE DeriveFunctor          #-}
{-# LANGUAGE FlexibleInstances      #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE KindSignatures         #-}
{-# LANGUAGE LambdaCase             #-}
{-# LANGUAGE OverloadedStrings      #-}
{-# LANGUAGE ScopedTypeVariables    #-}
{-# LANGUAGE TypeApplications       #-}
{-# LANGUAGE UndecidableInstances   #-}

module Transient.Core
    ( SqlValue(..)
    , SqlType'(..)
    , SqlType
    , module Transient.Core
    )
    where

import           Control.Monad               (join)
import qualified Data.ByteString             as BS
import           Data.Data                   (Data, constrFields, dataTypeOf,
                                              indexConstr)
import           Data.Int                    (Int64)
import qualified Data.List                   as List
import           Data.Map.Strict             (Map)
import qualified Data.Map.Strict             as Map
import           Data.String                 (IsString (..))
import qualified Data.Text                   as T
import           Data.Time.Clock             (UTCTime)
import           Data.Void                   (Void, absurd)
import           GHC.OverloadedLabels
import           GHC.TypeLits                (Symbol, symbolVal)

import           Transient.Internal.Backend
import           Transient.Internal.SqlType
import           Transient.Internal.SqlValue

type Encoder be a = a -> [SqlValue be]
type Decoder be a = [SqlValue be] -> Maybe (a, [SqlValue be])

fieldParser :: SqlType' be x a -> Decoder be a
fieldParser sqlType vs = do
    let vHead:vTail = vs
    r <- fromSqlType sqlType vHead
    pure (r, vTail)

data FieldDef = FieldDef
    { fieldName        :: BS.ByteString
    , fieldType        :: BS.ByteString
    , fieldConstraints :: [BS.ByteString]
    } deriving Show

data Table be i o = Table
    { tableEncoder  :: Encoder be i
    , tableDecoder  :: Decoder be o
    , tableName     :: BS.ByteString
    , tableFields   :: [FieldDef]
    , tableFieldMap :: Map String BS.ByteString
    }

data Fields be x i o = Fields
    { fieldsEncoder   :: Encoder be i
    , fieldsDecoder   :: Decoder be o
    , fieldsFieldDefs :: [FieldDef]
    } deriving Functor

instance Applicative (Fields be x i) where
    pure a = Fields
        { fieldsDecoder = \vs -> Just (a, vs)
        , fieldsEncoder = \_ -> []
        , fieldsFieldDefs = []
        }

    a <*> b = Fields
        { fieldsDecoder = \vals -> do
            (f, vals2) <- fieldsDecoder a vals
            (a, rest) <- fieldsDecoder b vals2
            pure (f a, rest)
        , fieldsEncoder = fieldsEncoder a <> fieldsEncoder b
        , fieldsFieldDefs = fieldsFieldDefs a <> fieldsFieldDefs b
        }

table :: forall o i be. Data o => BS.ByteString -> Fields be o i o -> Table be i o
table tableName fields =
    let tableType = dataTypeOf (undefined :: o)
        recordFields = constrFields $ indexConstr tableType 1
        fieldMap = Map.fromList $ zip recordFields (fmap fieldName $ fieldsFieldDefs fields)
    in
    Table
        { tableEncoder = fieldsEncoder fields
        , tableDecoder = fieldsDecoder fields
        , tableFields = fieldsFieldDefs fields
        , tableName = tableName
        , tableFieldMap = fieldMap
        }

data EntityField be ent i o = EntityField
    { entityFieldName :: BS.ByteString
    , entityFieldType :: SqlType' be i o
    }

class SqlField (field :: Symbol) be entity input output | be entity field -> input output where
    fieldDescriptor :: EntityField be entity input output

instance SqlField field be entity input output => IsLabel field (EntityField be entity input output) where
    fromLabel = fieldDescriptor @field

type SimpleField be rec a = EntityField be rec a a
instance AutoType be t => IsString (EntityField be r t t) where
    fromString s = EntityField (fromString s) auto

instance IsString (SqlType' be i o -> EntityField be r i o) where
    fromString s = EntityField (fromString s)

fieldDef :: EntityField be x i o -> FieldDef
fieldDef (EntityField fieldName t) =
    FieldDef fieldName (sqlType t) (sqlTypeConstraints t)

field :: (rec -> i) -> EntityField be x i o -> Fields be x rec o
field fromRec f@(EntityField _ t) =
    Fields
        { fieldsFieldDefs = [fieldDef f]
        , fieldsDecoder = fieldParser t
        , fieldsEncoder = List.singleton . toSqlType t CommandContext . fromRec
        }

createTable :: Backend be -> Table be i o -> IO Int64
createTable c t =
    let renderField f =
            fieldName f <> " " <> fieldType f <> " " <> BS.intercalate " " (fieldConstraints f)
        stmt = "CREATE TABLE " <> tableName t <> "("
                    <> BS.intercalate ", " (fmap renderField (tableFields t))
                    <> ")"
    in connExecute c stmt []

insertMany :: Backend be -> Table be i x -> [i] -> IO Int64
insertMany c t vals =
    let commas = BS.intercalate ", "
        fieldsPlaceholder fieldCount =
            "(" <> commas (replicate fieldCount "?") <> ")"
        fieldValues fieldCount =
            " VALUES " <> commas (replicate (length vals) (fieldsPlaceholder fieldCount))
    in connExecute c
        ("INSERT INTO " <> tableName t
                   <> " (" <> commas (fmap fieldName (tableFields t)) <> ")"
                   <> fieldValues (length $ tableFields t)) (join $ fmap (tableEncoder t) vals)

