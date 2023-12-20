SOURCE=postgres://$CREDENTIAL@data-center-prod.cluster-ro-cono0v2vbnhv.ap-southeast-1.rds.amazonaws.com:5432
TARGET=postgres://$CREDENTIAL@data-center-prod-cluster.cluster-ro-c2xpm1yjdga8.ap-southeast-1.rds.amazonaws.com:5432

python3 postgres/compare.py "$SOURCE" "$TARGET"
