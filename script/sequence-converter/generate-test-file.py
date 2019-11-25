import struct

values = [-1.1, 0.9, 1.0, 4.25]


def main():
    with open("output.bin", "wb+") as output:
        for value in values:
            output.write(bytearray(struct.pack("!f", value)))


if __name__ == "__main__":
    main()
