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
    ( SqlType'(..)
    , SqlType
    , module Transient.Core
    )
    where

import           Control.Monad                 (join)
import           Control.Monad.Reader          (ReaderT, ask, liftIO)
import qualified Data.ByteString               as BS
import qualified Data.ByteString.Builder       as BSB
import           Data.Data                     (Data, constrFields, dataTypeOf,
                                                indexConstr)
import           Data.Int                      (Int64)
import qualified Data.List                     as List
import           Data.Map.Strict               (Map)
import qualified Data.Map.Strict               as Map
import           Data.String                   (IsString (..))
import qualified Data.Text                     as T
import           Data.Time.Clock               (UTCTime)
import           Data.Void                     (Void, absurd)
import           GHC.OverloadedLabels
import           GHC.TypeLits                  (Symbol, symbolVal)

import           Transient.Internal.Backend
import           Transient.Internal.SqlDialect
import           Transient.Internal.SqlType

type Encoder sqlValue a = a -> [sqlValue]
type Decoder sqlValue a = [sqlValue] -> Maybe (a, [sqlValue])

fieldParser :: SqlType' sqlValue x a -> Decoder sqlValue a
fieldParser sqlType vs = do
    let vHead:vTail = vs
    r <- fromSqlType sqlType vHead
    pure (r, vTail)

data FieldDef = FieldDef
    { fieldName        :: BS.ByteString
    , fieldType        :: BS.ByteString
    , fieldConstraints :: [BS.ByteString]
    } deriving Show

data Table sqlValue i o = Table
    { tableEncoder  :: Encoder sqlValue i
    , tableDecoder  :: Decoder sqlValue o
    , tableName     :: BS.ByteString
    , tableFields   :: [FieldDef]
    , tableFieldMap :: Map String BS.ByteString
    }

data Fields sqlValue x i o = Fields
    { fieldsEncoder   :: Encoder sqlValue i
    , fieldsDecoder   :: Decoder sqlValue o
    , fieldsFieldDefs :: [FieldDef]
    } deriving Functor

instance Applicative (Fields sqlValue x i) where
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

table :: forall o i sqlValue. Data o => BS.ByteString -> Fields sqlValue o i o -> Table sqlValue i o
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

data EntityField sqlValue ent i o = EntityField
    { entityFieldName :: BS.ByteString
    , entityFieldType :: SqlType' sqlValue i o
    }

class SqlField (field :: Symbol) sqlValue entity input output | sqlValue entity field -> input output where
    fieldDescriptor :: EntityField sqlValue entity input output

instance SqlField field sqlValue entity input output => IsLabel field (EntityField sqlValue entity input output) where
    fromLabel = fieldDescriptor @field

type SimpleField sqlValue rec a = EntityField sqlValue rec a a
instance AutoType sqlValue t => IsString (EntityField sqlValue r t t) where
    fromString s = EntityField (fromString s) auto

instance IsString (SqlType' sqlValue i o -> EntityField sqlValue r i o) where
    fromString s = EntityField (fromString s)

fieldDef :: EntityField sqlValue x i o -> FieldDef
fieldDef (EntityField fieldName t) =
    let nullable = if not $ sqlTypeIsNullable t
                      then ["NOT NULL"]
                      else mempty
    in FieldDef fieldName (sqlType t) (nullable <> sqlTypeConstraints t)

field :: (rec -> i) -> EntityField sqlValue x i o -> Fields sqlValue x rec o
field fromRec f@(EntityField _ t) =
    Fields
        { fieldsFieldDefs = [fieldDef f]
        , fieldsDecoder = fieldParser t
        , fieldsEncoder = List.singleton . toSqlType t CommandContext . fromRec
        }

createTable :: Backend sqlValue-> Table sqlValue i o -> IO Int64
createTable c t =
    let renderField f =
            fieldName f <> " " <> fieldType f <> " " <> BS.intercalate " " (fieldConstraints f)
        stmt = "CREATE TABLE " <> tableName t <> "("
                    <> BS.intercalate ", " (fmap renderField (tableFields t))
                    <> ")"
    in connExecute c stmt []

runBuilder :: BSB.Builder -> BS.ByteString
runBuilder = BS.toStrict . BSB.toLazyByteString

commas :: [BSB.Builder] -> BSB.Builder
commas = mconcat . List.intersperse ", "

insertMany :: Table sqlValue i x -> [i] -> ReaderT (Backend sqlValue) IO Int64
insertMany t vals =
    insertInto t $ values t vals

newtype Insertion sqlValue i = Insertion { runInsertion :: SqlDialect -> (BSB.Builder, [sqlValue]) }

data ConsoleOutBackend

insertInto :: Table sqlValue i x -> Insertion sqlValue i -> ReaderT (Backend sqlValue) IO Int64
insertInto t insertion = do
    c <- ask
    let (insertionText, insertionValues) = runInsertion insertion (connDialect c)
    liftIO $ connExecute c
        (runBuilder ("INSERT INTO " <> BSB.byteString (dialectEscapeIdentifier (connDialect c) $ tableName t)
                <> " (" <> commas (fmap (BSB.byteString . dialectEscapeIdentifier (connDialect c) . fieldName) (tableFields t)) <> ") " <> insertionText))
        insertionValues

values :: Table sqlValue i x -> [i] -> Insertion sqlValue i
values t vals =
    let fieldCount = length $ tableFields t
        fieldsPlaceholder =
            "(" <> commas (replicate fieldCount "?") <> ")"
    in Insertion $ const $
        ("VALUES " <> commas (replicate (length vals) fieldsPlaceholder), join $ fmap (tableEncoder t) vals)
