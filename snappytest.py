import snappy
import sys
import optimized_snappy


def decompress(input_file_path):
    #decomp = snappy.StreamDecompressor()
    decomp = optimized_snappy.StreamDecompressor()
    with open(input_file_path, "rb") as input_file:
        while True:
            chunk = input_file.read(2 ** 20)
            if not chunk:
                decomp.flush()

            output = decomp.decompress(chunk)
            if not output:
                break


def compress(input_file_path, output_file_path):
    comp = snappy.StreamCompressor()
    with open(input_file_path, "rb") as input_file, open(output_file_path, "wb") as output_file:
        while True:
            chunk = input_file.read(2 ** 20)
            if not chunk:
                comp.flush()

            output = comp.compress(chunk)
            if not output:
                break

            output_file.write(output)


if __name__ == "__main__":
    foo = b"foobar"
    from snappy import _masked_crc32c
    from crc32c import crc32
    import binascii
    crc = crc32(foo)
    xx = (((crc >> 15) | (crc << 17)) + 0xa282ead8) & 0xffffffff
    print(_masked_crc32c(foo), xx)
    decompress(sys.argv[1])
    # compress(sys.argv[1], sys.argv[2])
