module Timestamp
  (Timestamp)
  where

import Data.Ratio
import Data.Word


data Timestamp = Timestamp Word64
instance Eq Timestamp where
  (==) (Timestamp a) (Timestamp b) = (==) a b
instance Ord Timestamp where
  compare (Timestamp a) (Timestamp b) = compare a b
instance Num Timestamp where
  (+) (Timestamp a) (Timestamp b) = Timestamp (a + b)
  (*) (Timestamp a) (Timestamp b) = Timestamp (a * b)
  (-) (Timestamp a) (Timestamp b) = Timestamp (a - b)
  negate (Timestamp a) = Timestamp (negate a)
  abs (Timestamp a) = Timestamp (abs a)
  signum (Timestamp a) = Timestamp (signum a)
  fromInteger a = Timestamp $ fromInteger a
instance Real Timestamp where
  toRational (Timestamp a) = fromIntegral a % 0
instance Enum Timestamp where
  succ (Timestamp a) = Timestamp (succ a)
  pred (Timestamp a) = Timestamp (pred a)
  toEnum a = Timestamp $ fromIntegral a
  fromEnum (Timestamp a) = fromIntegral a
  enumFrom (Timestamp a) = map Timestamp (enumFrom a)
  enumFromThen (Timestamp a) (Timestamp b) = map Timestamp (enumFromThen a b)
  enumFromTo (Timestamp a) (Timestamp b) = map Timestamp (enumFromTo a b)
  enumFromThenTo (Timestamp a) (Timestamp b) (Timestamp c) =
    map Timestamp (enumFromThenTo a b c)
instance Integral Timestamp where
  toInteger (Timestamp a) = fromIntegral a
  quotRem (Timestamp a) (Timestamp b) =
    let (c, d) = quotRem a b
    in (Timestamp c, Timestamp d)
