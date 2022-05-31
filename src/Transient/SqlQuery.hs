{-# LANGUAGE BlockArguments             #-}
{-# LANGUAGE DeriveFunctor              #-}
{-# LANGUAGE DerivingStrategies         #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE FunctionalDependencies     #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE TypeFamilies               #-}
{-# LANGUAGE TypeOperators              #-}
{-# LANGUAGE UndecidableInstances       #-}

module Transient.SqlQuery
    where

import           Control.Monad                 (join)
import           Control.Monad.State           (State, get, put, runState)
import qualified Data.ByteString               as BS
import           Data.Foldable                 (fold)
import           Data.Function                 ((&))
import qualified Data.List                     as List
import           Data.List.NonEmpty            (NonEmpty)
import qualified Data.List.NonEmpty            as NE
import           Data.Map                      (Map)
import qualified Data.Map                      as Map
import           Data.Maybe                    (listToMaybe, mapMaybe)
import           Data.String                   (IsString (..))

import           Transient.Core
import           Transient.Internal.Backend
import           Transient.Internal.SqlDialect


-- Imports for Test
import           Data.Int                      (Int64)
import           Data.Void                     (Void, absurd)

type QueryFragment = SqlDialect -> (BS.ByteString, [SqlValue])

instance IsString (BS.ByteString, [SqlValue]) where
    fromString s = (fromString s, [])


data SqlExpr a = SqlExpr
    { sqlExprSelect        :: SqlSelect a
    , sqlExprFragment      :: QueryFragment
    , sqlExprMultiFragment :: Maybe (NonEmpty QueryFragment)
    } deriving Functor

newtype From a = From
    { unFrom :: SqlQuery (QueryFragment, a) }

class AsFrom f r | f -> r where
    asFrom :: f -> From r
instance AsFrom (From a) a where
    asFrom = id

data SqlQueryState = SqlQueryState
    { sqsQueryParts    :: SqlQueryData
    , sqsIdentifierMap :: SqlQueryIdentifierMap
    }

type Ident = BS.ByteString
type SqlQueryIdentifierMap = Map Ident Int

freshIdent :: BS.ByteString -> SqlQuery Ident
freshIdent baseIdent = SqlQuery $ do
    sqs <- get
    case Map.lookup baseIdent (sqsIdentifierMap sqs) of
      Just count -> do
          let nextCount = count + 1
              ident = baseIdent <> (fromString (show nextCount))
              nextMap = Map.insert baseIdent nextCount $ sqsIdentifierMap sqs
          put sqs{sqsIdentifierMap = nextMap}
          pure ident
      Nothing -> do
          let nextMap = Map.insert baseIdent 1 $ sqsIdentifierMap sqs
          put sqs{sqsIdentifierMap = nextMap}
          pure baseIdent

data SqlQueryData = SqlQueryData
    { sqlQueryDataFromClause   :: Maybe [QueryFragment]
    , sqlQueryDataWhereClause  :: Maybe [QueryFragment]
    , sqlQueryDataHavingClause :: Maybe [QueryFragment]
    }

newtype SqlQuery a = SqlQuery
    { unSqlQuery :: State SqlQueryState a }
    deriving newtype (Functor, Applicative, Monad)

instance Semigroup SqlQueryData where
    (SqlQueryData a b c) <> (SqlQueryData a' b' c') = SqlQueryData (a <> a') (b <> b') (c <> c')
instance Monoid SqlQueryData where
    mempty = SqlQueryData mempty mempty mempty
    mappend = (<>)

data SqlSelect a = SqlSelect
    { sqlSelectQueryFragment :: QueryFragment
    , sqlSelectFromRow       :: FromRowFn a
    , sqlSelectColumnCount   :: Int
    } deriving Functor

instance Applicative SqlSelect where
    pure a =
        SqlSelect
            { sqlSelectQueryFragment = mempty
            , sqlSelectFromRow = const $ Just (a, [])
            , sqlSelectColumnCount = 0
            }
    a <*> b =
        SqlSelect
            { sqlSelectQueryFragment =
                \d -> sqlSelectQueryFragment (select_ a) d <> ", " <> sqlSelectQueryFragment (select_ b) d
            , sqlSelectFromRow = \vs -> do
                (f, v2) <- sqlSelectFromRow (select_ a) vs
                (x, vRest) <- sqlSelectFromRow (select_ b) v2
                pure (f x, vRest)
            , sqlSelectColumnCount = sqlSelectColumnCount (select_ a) + sqlSelectColumnCount (select_ b)
            }

maybeFromRow :: Int -> FromRowFn a -> FromRowFn (Maybe a)
maybeFromRow columnCount fromRow vals =
    if (all (== SqlNull) $ take columnCount vals) then
        pure (Nothing, drop columnCount vals)
    else do
        (x, r) <- fromRow vals
        pure (Just x, r)

nullableSqlSelect :: SqlSelect a -> SqlSelect (Maybe a)
nullableSqlSelect base =
    SqlSelect
        { sqlSelectQueryFragment = sqlSelectQueryFragment base
        , sqlSelectColumnCount = sqlSelectColumnCount base
        , sqlSelectFromRow = maybeFromRow (sqlSelectColumnCount base) (sqlSelectFromRow base)
        }

select :: AsSelect a r => a -> SelectQuery r
select = pure . select_

class AsSelect a r | a -> r where
    select_ :: a -> SqlSelect r

instance AsSelect (SqlSelect a) a where
    select_ = id

instance AsSelect (SqlExpr a) a where
    select_ = sqlExprSelect

instance (AsSelect a ar, AsSelect b br) => AsSelect (a, b) (ar, br) where
    select_ (a, b) =
            (,) <$> select_ a <*> select_ b

instance (AsSelect a ar, AsSelect b br, AsSelect c cr) => AsSelect (a, b, c) (ar, br, cr) where
    select_ (a, b, c) = (,,) <$> select_ a <*> select_ b <*> select_ c

instance ( AsSelect a ar , AsSelect b br
         , AsSelect c cr , AsSelect d dr
         ) => AsSelect (a, b, c, d) (ar, br, cr, dr) where
    select_ (a, b, c, d) =
                (,,,) <$> select_ a <*> select_ b
                      <*> select_ c <*> select_ d

instance ( AsSelect a ar , AsSelect b br
         , AsSelect c cr , AsSelect d dr
         , AsSelect e er
         ) => AsSelect (a, b, c, d, e) (ar, br, cr, dr, er) where
    select_ (a, b, c, d, e) =
                (,,,,) <$> select_ a <*> select_ b
                       <*> select_ c <*> select_ d
                       <*> select_ e

instance ( AsSelect a ar , AsSelect b br , AsSelect c cr
         , AsSelect d dr , AsSelect e er , AsSelect f fr
         ) => AsSelect (a, b, c, d, e, f) (ar, br, cr, dr, er, fr) where
    select_ (a, b, c, d, e, f) =
                (,,,,,) <$> select_ a <*> select_ b <*> select_ c
                        <*> select_ d <*> select_ e <*> select_ f

instance ( AsSelect a ar , AsSelect b br , AsSelect c cr , AsSelect d dr
         , AsSelect e er , AsSelect f fr , AsSelect g gr
         ) => AsSelect (a, b, c, d, e, f, g) (ar, br, cr, dr, er, fr, gr) where
    select_ (a, b, c, d, e, f, g) =
                (,,,,,,) <$> select_ a <*> select_ b <*> select_ c <*> select_ d
                         <*> select_ e <*> select_ f <*> select_ g

instance ( AsSelect a ar , AsSelect b br , AsSelect c cr , AsSelect d dr
         , AsSelect e er , AsSelect f fr , AsSelect g gr , AsSelect h hr
         ) => AsSelect (a, b, c, d, e, f, g, h) (ar, br, cr, dr, er, fr, gr, hr) where
    select_ (a, b, c, d, e, f, g, h) =
                (,,,,,,,) <$> select_ a <*> select_ b <*> select_ c <*> select_ d
                          <*> select_ e <*> select_ f <*> select_ g <*> select_ h

instance ( AsSelect a ar , AsSelect b br , AsSelect c cr
         , AsSelect d dr , AsSelect e er , AsSelect f fr
         , AsSelect g gr , AsSelect h hr , AsSelect i ir
         ) => AsSelect (a, b, c, d, e, f, g, h, i) (ar, br, cr, dr, er, fr, gr, hr, ir) where
    select_ (a, b, c, d, e, f, g, h, i) =
                (,,,,,,,,) <$> select_ a <*> select_ b <*> select_ c
                           <*> select_ d <*> select_ e <*> select_ f
                           <*> select_ g <*> select_ h <*> select_ i

type SelectQuery r = SqlQuery (SqlSelect r)

runSelectUnique :: Backend -> SelectQuery r -> IO (Maybe r)
runSelectUnique conn q = fmap listToMaybe $ runSelectMany conn q

unSelectQuery :: SelectQuery a -> (SqlSelect a, SqlQueryState)
unSelectQuery q =
    runState (unSqlQuery q) (SqlQueryState mempty mempty)

runSelectMany :: Backend -> SelectQuery r -> IO [r]
runSelectMany conn q =
    let (sqlSelect, SqlQueryState qData _) = unSelectQuery q
        (qText, qVals) = renderSelect (connDialect conn) sqlSelect qData
    in do
        rVals <- qText `seq` qVals `seq` connQuery conn qText qVals
        pure $ flip mapMaybe rVals $ \vals -> do
            (v, []) <- sqlSelectFromRow sqlSelect vals
            pure v

renderSelect :: SqlDialect -> SqlSelect a -> SqlQueryData -> (BS.ByteString, [SqlValue])
renderSelect d sqlSelect qd =
    fold
        [ "SELECT "
        , sqlSelectQueryFragment sqlSelect d
        , maybe mempty (" FROM " <>) (fmap (fold . List.intersperse " CROSS JOIN LATERAL " . fmap ($ d)) $ sqlQueryDataFromClause qd)
        , maybe mempty (" WHERE " <>) (fmap (fold . List.intersperse " AND " . fmap ($ d)) $ sqlQueryDataWhereClause qd)
        , maybe mempty (" HAVING " <>) (fmap (fold . List.intersperse " AND " . fmap ($ d)) $ sqlQueryDataHavingClause qd)
        ]

from :: AsFrom f r => f -> SqlQuery r
from f = do
    (fromClause, res) <- unFrom $ asFrom f
    writeQueryData mempty{sqlQueryDataFromClause = Just [fromClause]}
    pure res

writeQueryData :: SqlQueryData -> SqlQuery ()
writeQueryData queryData =
    SqlQuery $ do
        sqs <- get
        put $ sqs{ sqsQueryParts = sqsQueryParts sqs <> queryData }

where_ :: SqlExpr Bool -> SqlQuery ()
where_ expr =
    writeQueryData mempty{sqlQueryDataWhereClause = Just [sqlExprFragment expr]}

simpleExpr :: QueryFragment -> FromRowFn a -> SqlExpr a
simpleExpr frag fromRow =
    SqlExpr
        { sqlExprSelect = SqlSelect frag fromRow 1
        , sqlExprFragment = frag
        , sqlExprMultiFragment = Nothing
        }

binOp :: BS.ByteString -> BS.ByteString -> FromRowFn c -> SqlExpr a -> SqlExpr b -> SqlExpr c
binOp op connect fromRow lhs rhs =
    let applyOp :: QueryFragment -> QueryFragment -> QueryFragment
        applyOp l r = \d -> l d <> (" " <> op <> " ", []) <> r d
        frag = case (sqlExprMultiFragment lhs, sqlExprMultiFragment rhs) of
              (Just ls, Just rs) ->
                       fold $
                       NE.intersperse (const (" " <> connect <> " ", [])) $
                       fmap (uncurry applyOp) $
                       NE.zip ls rs

              _ -> applyOp (sqlExprFragment lhs) (sqlExprFragment rhs)
    in simpleExpr frag fromRow

boolFromRow :: FromRowFn Bool
boolFromRow = fieldParser auto

infixr 6 ==., /=.
(==.) :: SqlExpr a -> SqlExpr a -> SqlExpr Bool
(==.) = binOp "=" "AND" boolFromRow
(/=.) :: SqlExpr a -> SqlExpr a -> SqlExpr Bool
(/=.) = binOp "!=" "OR" boolFromRow

infix 4 `and_`, `or_`
and_ :: SqlExpr Bool -> SqlExpr Bool -> SqlExpr Bool
and_ = binOp "AND" "AND" boolFromRow
or_ :: SqlExpr Bool -> SqlExpr Bool -> SqlExpr Bool
or_ = binOp "OR" "OR" boolFromRow

isNull_ :: SqlExpr (Maybe a) -> SqlExpr Bool
isNull_ expr =
    let frag =
            case sqlExprMultiFragment expr of
              Just fs ->
                   fold $
                   NE.intersperse (const " AND ") $
                   fmap (<> const " IS NULL") fs
              Nothing ->
                   sqlExprFragment expr <> const " IS NULL"
    in simpleExpr frag boolFromRow

isNotNull_ :: SqlExpr (Maybe a) -> SqlExpr Bool
isNotNull_ expr =
    let frag =
            case sqlExprMultiFragment expr of
              Just fs ->
                   fold $
                   NE.intersperse (const " OR ") $
                   fmap (<> const " IS NOT NULL") fs
              Nothing ->
                   sqlExprFragment expr <> const " IS NOT NULL"
    in simpleExpr frag boolFromRow

val :: AutoType i => i -> SqlExpr i
val x = val' x auto

val' :: i -> SqlType' i o -> SqlExpr o
val' x sqlType =
    let v = toSqlType sqlType x
    in simpleExpr (const ("?", [v])) (fieldParser sqlType)

just_ :: SqlExpr a -> SqlExpr (Maybe a)
just_ = fmap Just

join_ :: SqlExpr (Maybe (Maybe a)) -> SqlExpr (Maybe a)
join_ = fmap join

instance AsFrom (Table i r) (SqlExpr r) where
    asFrom = table_

table_ :: Table x r -> From (SqlExpr r)
table_ model = From $ do
    ident <- freshIdent (tableName model)
    let fromFragment = if ident == (tableName model)
                          then \d -> (dialectEscapeIdentifier d ident, [])
                          else \d -> ((dialectEscapeIdentifier d (tableName model))
                                        <> " AS " <> (dialectEscapeIdentifier d ident), [])
        fields = fmap (\f d -> (dialectEscapeIdentifier d ident) <> "." <> (dialectEscapeIdentifier d (fieldName f))) $ tableFields model
        selectFragment = \d -> (BS.intercalate ", " (fmap ($d) fields), [])
    pure ( fromFragment
         , SqlExpr
            { sqlExprFragment = \d -> (dialectEscapeIdentifier d ident, [])
            , sqlExprMultiFragment = NE.nonEmpty $ fmap (\x -> \d -> (x d,[])) fields
            , sqlExprSelect = SqlSelect (selectFragment) (tableFromRow model) (length $ tableFields model)
            }
         )

type OnClause a = a -> SqlExpr Bool

infix 9 `on_`
on_ :: (OnClause a -> b) -> OnClause a -> b
on_ = ($)

data a :& b = a :& b
infixl 2 :&

class ToMaybe arg where
    type ToMaybeT arg
    toMaybe :: arg -> ToMaybeT arg

instance ToMaybe (SqlExpr a) where
    type ToMaybeT (SqlExpr a) = SqlExpr (Maybe a)
    toMaybe base =
        SqlExpr
            { sqlExprSelect = nullableSqlSelect (sqlExprSelect base)
            , sqlExprFragment = sqlExprFragment base
            , sqlExprMultiFragment = sqlExprMultiFragment base
            }
instance (ToMaybe a, ToMaybe b) => ToMaybe (a :& b) where
    type ToMaybeT (a :& b) = ToMaybeT a :& ToMaybeT b
    toMaybe (a :& b) = toMaybe a :& toMaybe b

type Join fa a fb b c = fb -> OnClause (a :& b) -> fa -> From c

joinHelper :: (AsFrom fa a, AsFrom fb b)
           => BS.ByteString
           -> (a -> b -> c)
           -> Join fa a fb b c
joinHelper joinType toRes joinFrom onClause baseFrom = From do
    (baseFromFragment, baseExpr) <- unFrom $ asFrom baseFrom
    (joinFromFragment, joinExpr) <- unFrom $ asFrom joinFrom
    let onExpr = onClause (baseExpr :& joinExpr)
    pure ( \d -> baseFromFragment d <> (" " <> joinType <> " ", []) <> joinFromFragment d <> " ON " <> sqlExprFragment onExpr d
         , toRes baseExpr joinExpr
         )

innerJoin_ :: (AsFrom fa a, AsFrom fb b) => Join fa a fb b (a :& b)
innerJoin_ = joinHelper "INNER JOIN" (:&)

leftJoin_ :: (AsFrom fa a, AsFrom fb b, ToMaybe b)
          => Join fa a fb b (a :& ToMaybeT b)
leftJoin_ = joinHelper "LEFT JOIN" (\a b -> a :& toMaybe b)

infixr 9 ^., ?., ??.
(^.) :: SqlExpr rec -> EntityField rec i o -> SqlExpr o
(^.) = projectField id

(?.) :: SqlExpr (Maybe rec) -> EntityField rec i o -> SqlExpr (Maybe o)
(?.) = projectField (maybeFromRow 1)

(??.) :: SqlExpr (Maybe rec) -> EntityField rec i (Maybe o) -> SqlExpr (Maybe o)
(??.) = projectField id

projectField :: (FromRowFn d -> FromRowFn e) -> SqlExpr a -> EntityField b c d -> SqlExpr e
projectField f ent (EntityField name sqlType) =
    let frag = \d -> sqlExprFragment ent d <> "." <> (dialectEscapeIdentifier d name, [])
        fromRow = fieldParser sqlType
    in simpleExpr frag (f fromRow)
