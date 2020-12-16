from struct import pack, unpack


def write_pair(buffer_write, key, value):
    key_bytes = key.encode('utf-8')
    value_bytes = value.encode('utf-8')
    buffer_write(pack("I", len(key_bytes)))
    buffer_write(key_bytes)
    buffer_write(pack("I", len(value_bytes)))
    buffer_write(value_bytes)


def read_pairs(buffer_read):
    while key_len_bytes := buffer_read(4):
        (key_len, ) = unpack('I', key_len_bytes)
        key_bytes = buffer_read(key_len)
        (value_len, ) = unpack('I', buffer_read(4))
        value_bytes = buffer_read(value_len)
        yield key_bytes.decode('utf-8'), value_bytes.decode('utf-8')
