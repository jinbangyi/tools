export CREDENTIAL=postgres:

SOURCE=postgres://$CREDENTIAL@data-center-prod.cluster-ro-cono0v2vbnhv.ap-southeast-1.rds.amazonaws.com:5432
TARGET=postgres://$CREDENTIAL@data-center-prod10-cluster.cluster-ro-c2xpm1yjdga8.ap-southeast-1.rds.amazonaws.com:5432

export CREDENTIAL=postgres:
SOURCE=postgres://$CREDENTIAL@um-pg2-prod.cluster-ro-cono0v2vbnhv.ap-southeast-1.rds.amazonaws.com:5432
TARGET=postgres://$CREDENTIAL@user-manager-prod5-cluster.cluster-ro-c2xpm1yjdga8.ap-southeast-1.rds.amazonaws.com:5432

export CREDENTIAL=postgres:
SOURCE=postgres://$CREDENTIAL@aurora-blockchain-fetcher-prod.cluster-ro-cono0v2vbnhv.ap-southeast-1.rds.amazonaws.com:5432
TARGET=postgres://$CREDENTIAL@blockchain-fetcher-prod3-cluster.cluster-ro-c2xpm1yjdga8.ap-southeast-1.rds.amazonaws.com:5432

export PYTHONPATH=`pwd`
python3 postgres/compare.py "$SOURCE" "$TARGET"
