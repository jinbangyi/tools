export PYTHONPATH=`pwd`
export SOURCE=prod-ro.q30krv.ng.0001.apse1.cache.amazonaws.com
export TARGET=data-farmer2-prod.y0qx7w.ng.0001.apse1.cache.amazonaws.com

# python3.11 -m pip install redis click loguru
python3.11 redis/counter.py "$SOURCE" "$TARGET"
