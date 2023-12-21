SOURCE=postgres://$CREDENTIAL@data-center-prod.cluster-ro-cono0v2vbnhv.ap-southeast-1.rds.amazonaws.com:5432
TARGET=postgres://$CREDENTIAL@data-center-prod-cluster.cluster-ro-c2xpm1yjdga8.ap-southeast-1.rds.amazonaws.com:5432

SOURCE=postgres://$CREDENTIAL@um-pg2-prod.cluster-ro-cono0v2vbnhv.ap-southeast-1.rds.amazonaws.com:5432
TARGET=postgres://$CREDENTIAL@user-manager-prod.cluster-ro-c2xpm1yjdga8.ap-southeast-1.rds.amazonaws.com:5432

SOURCE=postgres://$CREDENTIAL@aurora-blockchain-fetcher-prod.cluster-ro-cono0v2vbnhv.ap-southeast-1.rds.amazonaws.com:5432
TARGET=postgres://$CREDENTIAL@blockchain-fetcher-prod6-cluster.cluster-ro-c2xpm1yjdga8.ap-southeast-1.rds.amazonaws.com:5432

python3 postgres/compare.py "$SOURCE" "$TARGET"
