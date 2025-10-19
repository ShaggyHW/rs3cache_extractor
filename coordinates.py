# convert_coords.py

def convert_to_coordinates(input_file, output_file):
    with open(input_file, 'r') as f:
        lines = f.readlines()

    coordinates = []
    for line in lines:
        parts = [part.strip() for part in line.strip().split(',')]
        if len(parts) == 3:
            x, y, z = parts
            coordinates.append(f"    new Coordinate({x}, {y}, {z})")

    with open(output_file, 'w') as f:
        f.write("Coordinate[] path = {\n")
        f.write(",\n".join(coordinates))
        f.write("\n};\n")

    print(f"✅ Converted {len(coordinates)} coordinates and saved to '{output_file}'.")


if __name__ == "__main__":
    # Example usage
    input_file = "input.txt"   # your file with lines like "3240 3731 0"
    output_file = "output.txt" # new file to save formatted code

    convert_to_coordinates(input_file, output_file)