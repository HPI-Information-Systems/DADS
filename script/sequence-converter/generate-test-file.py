import struct

values = [4.4, 5.5, 6.6, 7.7]


def main():
    with open("output.bin", "wb+") as output:
        for value in values:
            output.write(bytearray(struct.pack("!d", value)))


if __name__ == "__main__":
    main()
