module ClickHouseDriver.Utils.Datetimes (

) where

data Date = Date {
    year :: {-# UNPACK #-} !Int,
    month :: {-# UNPACK #-} !Int,
    day :: {-# UNPACK #-} !Int
}

instance Show Date where
    show Date{year = y, month = m, day = d} = show y ++ "-" ++ show m ++ "-" ++ show d

instance Num Date where
    (*) = undefined
    Date y1 m1 d1 + Date y2 m2 d2 = Date (y1 + y2) (m1 + m2) (d1 + d2)
    Date y1 m1 d1 - Date y2 m2 d2 = Date (y1 - y2) (m1 - m2) (d1 - d2)
    
