import argparse
import json

from sparkaid import json_schema_to_spark_schema


def convert_json_schema_to_spark_schema(input, output):
    """
    :param input: input filename
    :param output: output filename
    :return:
    """
    with open(input, "r") as in_f:
        schema = json_schema_to_spark_schema(json.load(in_f))
        with open(output, "w") as out_f:
            out_f.write(json.dumps(json.loads(schema.json()), indent=4))


def main():
    parser = argparse.ArgumentParser(prog='PROG')
    sub_parsers = parser.add_subparsers(help='sub-command help', dest='command')
    parser_json_schema = sub_parsers.add_parser('json_schema_to_spark', help='Convert spark schema to JSON schema')
    parser_json_schema.add_argument('-i', '--input', type=str, required=True,
                                    help='Input file. Content should be a JSON string')
    parser_json_schema.add_argument('-o', '--output', type=str, required=True,
                                    help='Output file. Content will be a JSON string')

    args = parser.parse_args()

    if args.command == 'json_schema_to_spark':
        convert_json_schema_to_spark_schema(args.input, args.output)


if __name__ == "__main__":
    main()
