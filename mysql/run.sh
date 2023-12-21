SOURCE=mysql+pymysql://://$CREDENTIAL@prod.cono0v2vbnhv.ap-southeast-1.rds.amazonaws.com:3306
TARGET=mysql+pymysql://://$CREDENTIAL@prod.c2xpm1yjdga8.ap-southeast-1.rds.amazonaws.com:3306

python3 mysql/compare.py "$SOURCE" "$TARGET"
