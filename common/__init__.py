from loguru import logger


exit_code = 0


def log(msg: str, obj, pretty: bool = False):
    global exit_code
    if obj:
        if pretty:
            for k, v in obj.get('values_changed', {}).items():
                v['new_value'] = round(v['new_value'] / pow(1024, 3), 2)
                v['old_value'] = round(v['old_value'] / pow(1024, 3), 2)
                v['delta'] = round(abs(v['new_value'] - v['old_value']), 2)
        # if pretty == 'bytes':
        #     obj = obj / pow(1024, 3)
        logger.info(f'{msg}: {obj}G')
        exit_code += 1


def size_compare(source: dict, target: dict, gap: int):
    new_source = {}
    new_target = {}
    for k in source.keys():
        if k in target:
            if abs(source.get(k) - target.get(k)) > gap:
                new_source[k] = source.get(k)
                new_target[k] = target.get(k)
        else:
            new_source[k] = source.get(k)

    [new_target.setdefault(i, target.get(i)) for i in set(target.keys()) - set(source.keys())]

    return new_source, new_target
