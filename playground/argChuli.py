import argparse

parser = argparse.ArgumentParser()

parser.add_argument("echo", help="echo the string you used here")
parser.add_argument("--verbose", action="store_true", help="increase output verbosity")

args = parser.parse_args()
print args.echo
print args.verbose
