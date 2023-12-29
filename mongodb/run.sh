SOURCE=mongodb+srv://$CREDENTIAL@prod.ibu3n.mongodb.net
TARGET=mongodb+srv://$CREDENTIAL@data-farmer.ibu3n.mongodb.net

SOURCE=mongodb+srv://$CREDENTIAL@orderbook.ibu3n.mongodb.net
TARGET=mongodb+srv://$CREDENTIAL@orderbook2.ibu3n.mongodb.net

SOURCE=mongodb+srv://$CREDENTIAL@data-platform-v2.ibu3n.mongodb.net
TARGET=mongodb+srv://$CREDENTIAL@data-platform.ibu3n.mongodb.net

SOURCE=mongodb://$CREDENTIAL@data-api-a.mongodb.nftgo.io:27017,data-api-b.mongodb.nftgo.io:27017,data-api-c.mongodb.nftgo.io:27017
TARGET=mongodb://$CREDENTIAL@10.3.98.107:27017,10.3.96.105:27017,10.3.99.211:27017

SOURCE=mongodb://$CREDENTIAL@bot-prod.cluster-cono0v2vbnhv.ap-southeast-1.docdb.amazonaws.com:27017
TARGET=mongodb://$CREDENTIAL@pricing-prod5.cluster-c2xpm1yjdga8.ap-southeast-1.docdb.amazonaws.com:27017

python3 mongodb/compare.py "$SOURCE" "$TARGET"


# overwrite collection data of a special time range
SOURCE=mongodb+srv://$CREDENTIAL@prod.ibu3n.mongodb.net
TARGET=mongodb+srv://$CREDENTIAL@data-farmer.ibu3n.mongodb.net
python3 mongodb/overwrite.py "$SOURCE" "$TARGET" --collection=
