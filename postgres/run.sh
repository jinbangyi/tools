SOURCE=postgres://$CREDENTIAL@data-center-prod.cluster-ro-cono0v2vbnhv.ap-southeast-1.rds.amazonaws.com:5432/data_center
TARGET=postgres://$CREDENTIAL@data-center-prod-cluster.cluster-ro-c2xpm1yjdga8.ap-southeast-1.rds.amazonaws.com:5432/data_center

python3 postgres/compare.py "$SOURCE" "$TARGET"
